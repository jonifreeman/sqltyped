package sqltyped

import schemacrawler.schemacrawler._
import schemacrawler.schema.{ColumnDataType, Schema}
import schemacrawler.utility.SchemaCrawlerUtility
import scala.reflect.runtime.universe.{Type, typeOf}
import Ast._

case class TypedValue(name: String, tpe: Type, nullable: Boolean, tag: Option[String])

case class TypedStatement(
    input: List[TypedValue]
  , output: List[TypedValue]
  , stmt: Statement
  , uniqueConstraints: Map[Table, List[List[Column]]]
  , generatedKeyTypes: List[TypedValue]
  , multipleResults: Boolean = true)

object DbSchema {
  def read(url: String, driver: String, username: String, password: String): Result[Schema] = try {
    Class.forName(driver)
    val options = new SchemaCrawlerOptions
    val level = new SchemaInfoLevel
    level.setRetrieveTables(true)
    level.setRetrieveColumnDataTypes(true)
    level.setRetrieveTableColumns(true)
    level.setRetrieveIndices(true)
    level.setRetrieveForeignKeys(true)
    options.setSchemaInfoLevel(level)
    val schemaName = url.split('?')(0).split('/').reverse.head
    options.setSchemaInclusionRule(new InclusionRule(schemaName, ""))
    val conn = getConnection(url, username, password)
    val database = SchemaCrawlerUtility.getDatabase(conn, options)
    database.getSchema(schemaName).ok
  } catch {
    case e: Exception => fail(e.getMessage)
  }

  private def getConnection(url: String, username: String, password: String) =
    java.sql.DriverManager.getConnection(url, username, password)
}

object Typer {
  def infer(schema: Schema, stmt: Statement, useInputTags: Boolean): Result[TypedStatement] = {
    def tag(col: Column) = {
      val table = col.resolvedTable getOrElse sys.error("Column's table not resolved " + col)
      getTable(schema, table) map { t =>
        def findFK = t.getForeignKeys
          .flatMap(_.getColumnPairs.map(_.getForeignKeyColumn))
          .find(_.getName == col.name)
          .map(_.getReferencedColumn.getParent.getName)

        if (t.getPrimaryKey != null && t.getPrimaryKey.getColumns.exists(_.getName == col.name))
          Some(table.name)
        else findFK orElse None
      }
    }

    def typeValue(inputArg: Boolean, useTags: Boolean)(x: Value): Result[List[TypedValue]] = x match {
      case col: Column => 
        for {
          (tpe, inopt, outopt) <- inferColumnType(schema, stmt, col)
          t <- tag(col)
        } yield List(TypedValue(col.aname, tpe, if (inputArg) inopt else outopt, if (useTags) t else None))
      case cols@AllColumns(_, t) =>
        val table = t getOrElse sys.error("Table not resolved for " + cols)
        for {
          tbl <- getTable(schema, table)
          cs  <- sequence(tbl.getColumns.toList map { c => typeValue(inputArg, useTags)(Column(c.getName, None, None, Some(table))) })
        } yield cs.flatten
      case f@Function(name, params, alias) =>
        inferReturnType(schema, stmt, name, params) map { case (tpe, inopt, outopt) =>
          List(TypedValue(f.aname, tpe, if (inputArg) inopt else outopt, None))
        }
      case c@Constant(tpe, _) => List(TypedValue("<constant>", tpe, false, None)).ok
      case ArithExpr(lhs, _, rhs) => for {
        lval <- typeValue(inputArg, useTags)(lhs)
        rval <- typeValue(inputArg, useTags)(rhs)
      } yield
        (lval.head, rval.head) match {
          case (col: Column, _) => List(col)
          case (_, col: Column) => List(col)
          case (c: Constant, _) if c.tpe == typeOf[Double] => List(c)
          case (_, c: Constant) if c.tpe == typeOf[Double] => List(c)
          case (c: Constant, _) => List(TypedValue("<constant>", typeOf[Int], false, None))
          case (_, c: Constant) => List(TypedValue("<constant>", typeOf[Int], false, None))
          case _ => lval
        }
    }

    def uniqueConstraints = {
      val constraints = sequence(stmt.tables map { t =>
        getTable(schema, t) map { table =>
          val indices = Option(table.getPrimaryKey).map(List(_)).getOrElse(Nil) ::: table.getIndices.toList
          val uniques = indices filter (_.isUnique) map { i =>
            i.getColumns.toList.map(col => Column(col.getName, Some(t.name), None, Some(t)))
          }
          (t, uniques)
        }
      })

      constraints map (cs => Map[Table, List[List[Column]]]().withDefault(_ => Nil) ++ cs)
    }

    def generatedKeyTypes(table: Table) = for {
      t <- getTable(schema, table)
    } yield {
      def tag(c: schemacrawler.schema.Column) = 
        Option(t.getPrimaryKey).flatMap(_.getColumns.find(_.getName == c.getName)).map(_ => t.getName)

      t.getColumns.toList
        .filter(c => c.getType.isAutoIncrementable)
        .map(c => TypedValue(c.getName, mkType(c.getType), false, tag(c)))
    }

    for {
      in  <- sequence(stmt.input(schema) map typeValue(inputArg = true, useTags = useInputTags))
      out <- sequence(stmt.output map typeValue(inputArg = false, useTags = true))
      ucs <- uniqueConstraints
      key <- generatedKeyTypes(stmt.tables.head)
    } yield TypedStatement(in.flatten, out.flatten, stmt, ucs, key)
  }

  def `a => a` = (schema: Schema, stmt: Statement, params: List[Term]) =>
    if (params.length != 1) fail("Expected 1 parameter " + params)
    else 
      tpeOf(schema, stmt, params.head) map { case (tpe, inopt, _) => (tpe, inopt, true) }

  // FIXME make this extensible
  val knownFunctions = Map(
      "abs"   -> `a => a`
    , "avg"   -> ((_: Schema, _: Statement, _: List[Term]) => (typeOf[Double], false, true).ok)
    , "count" -> ((_: Schema, _: Statement, _: List[Term]) => (typeOf[Long], false, false).ok)
    , "min"   -> `a => a`
    , "max"   -> `a => a`
    , "sum"   -> `a => a`
    , "upper" -> ((_: Schema, _: Statement, _: List[Term]) => (typeOf[String], false, true).ok)
  )

  def tpeOf(schema: Schema, stmt: Statement, e: Term): Result[(Type, Boolean, Boolean)] = e match {
    case Constant(tpe, _)       => (tpe, false, false).ok
    case col: Column            => inferColumnType(schema, stmt, col)
    case Function(n, params, _) => inferReturnType(schema, stmt, n, params)
    case x                      => sys.error("Term " + x + " not supported")
  }

  def inferReturnType(schema: Schema, stmt: Statement, fname: String, params: List[Term]) = 
    knownFunctions.get(fname.toLowerCase) match {
      case Some(f) => f(schema, stmt, params)
      case None => (typeOf[AnyRef], true, true).ok
    }

  def inferColumnType(schema: Schema, stmt: Statement, col: Column) = {
    val table = col.resolvedTable getOrElse sys.error("Table not resolved for " + col)
    for {
      t <- getTable(schema, table)
      c <- Option(t.getColumn(col.name)) resultOrFail ("No such column " + col)
    } yield (mkType(c.getType), c.isNullable, c.isNullable)
  }

  private def getTable(schema: Schema, table: Table) =
    Option(schema.getTable(table.name)) resultOrFail ("Unknown table " + table.name)

  private def mkType(t: ColumnDataType): Type = t.getTypeClassName match {
    case "java.lang.String" => typeOf[String]
    case "java.lang.Short" => typeOf[Short]
    case "java.lang.Integer" => typeOf[Int]
    case "java.lang.Long" => typeOf[Long]
    case "java.lang.Float" => typeOf[Float]
    case "java.lang.Double" => typeOf[Double]
    case "java.lang.Boolean" => typeOf[Boolean]
    case "java.lang.Byte" => typeOf[Byte]
    case "java.sql.Timestamp" => typeOf[java.sql.Timestamp]
    case "java.sql.Date" => typeOf[java.sql.Date]
    case "java.sql.Time" => typeOf[java.sql.Time]
    case "byte[]" => typeOf[java.sql.Blob]
    case x => sys.error("Unknown type " + x)  // FIXME improve error handling
  }
}
