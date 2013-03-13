package sqltyped

import java.sql._
import shapeless._
import scala.reflect.macros._
import schemacrawler.schema.Schema
import NumOfResults._

case class Configuration[A, B](tables: A, columns: B)

object SqlMacro {
  private val schemaCache = new java.util.WeakHashMap[Context#Run, ?[Schema]]()

  def withResultSet[A](stmt: PreparedStatement)(f: ResultSet => A) = {
    var rs: ResultSet = null
    try {
      f(stmt.executeQuery)
    } finally {
      if (rs != null) try rs.close catch { case e: Exception => }
      stmt.close
    }
  }

  def withStatement(stmt: PreparedStatement) = 
    try {
      stmt.executeUpdate
    } finally {
      stmt.close
    }

  def withStatementF[A](stmt: PreparedStatement)(f: => A): A = 
    try {
      stmt.executeUpdate
      f
    } finally {
      stmt.close
    }

  def sqlImpl[A: c.WeakTypeTag, B: c.WeakTypeTag]
      (c: Context)
      (s: c.Expr[String])
      (config: c.Expr[Configuration[A, B]]): c.Expr[Any] = 
    sqlImpl0(c, useInputTags = false, keys = false)(s)(config)

  def sqltImpl[A: c.WeakTypeTag, B: c.WeakTypeTag]
      (c: Context)
      (s: c.Expr[String])
      (config: c.Expr[Configuration[A, B]]): c.Expr[Any] = 
    sqlImpl0(c, useInputTags = true, keys = false)(s)(config)

  def sqlkImpl[A: c.WeakTypeTag, B: c.WeakTypeTag]
      (c: Context)
      (s: c.Expr[String])
      (config: c.Expr[Configuration[A, B]]): c.Expr[Any] = 
    sqlImpl0(c, useInputTags = false, keys = true)(s)(config)

  def sqlImpl0[A: c.WeakTypeTag, B: c.WeakTypeTag]
      (c: Context, useInputTags: Boolean, keys: Boolean)
      (s: c.Expr[String])
      (config: c.Expr[Configuration[A, B]]): c.Expr[Any] = {

    import c.universe._

    val sql = s.tree match {
      case Literal(Constant(sql: String)) => sql
      case _ => c.abort(c.enclosingPosition, "Argument to macro must be a String literal")
    }
    compile(c, useInputTags, keys, inputsInferred = true, 
            sql, (p, s) => p.parseAllWith(p.stmt, s))(config, Literal(Constant(sql)))
  }

  def dynsqlImpl[A: c.WeakTypeTag, B: c.WeakTypeTag]
      (c: Context)(exprs: c.Expr[Any]*)
      (config: c.Expr[Configuration[A, B]]): c.Expr[Any] = {

    import c.universe._

    def append(t1: Tree, t2: Tree) = Apply(Select(t1, newTermName("+").encoded), List(t2))

    val Expr(Apply(_, List(Apply(_, parts)))) = c.prefix

    val select = parts.head
    val sqlExpr = exprs.zip(parts.tail).foldLeft(select) { 
      case (acc, (Expr(expr), part)) => append(acc, append(expr, part)) 
    }

    val sql = select match {
      case Literal(Constant(sql: String)) => sql
      case _ => c.abort(c.enclosingPosition, "Expected String literal as first part of interpolation")
    }

    compile(c, useInputTags = false, keys = false, inputsInferred = false,
            sql, (p, s) => p.parseWith(p.selectStmt, s))(config, sqlExpr)
  }

  def compile[A: c.WeakTypeTag, B: c.WeakTypeTag]
      (c: Context, useInputTags: Boolean, keys: Boolean, inputsInferred: Boolean, sql: String,
       parse: (SqlParser, String) => ?[Ast.Statement[Option[String]]])
      (config: c.Expr[Configuration[A, B]], sqlExpr: c.Tree): c.Expr[Any] = {

    import c.universe._

    def sysProp(n: String) = scala.util.Properties.propOrNone(n) orFail 
        "System property '" + n + "' is required to get a compile time connection to the database"

    def cachedSchema(config: DbConfig) = {
      val cached = schemaCache.get(c.enclosingRun)
      if (cached != null) cached else {
        val s = DbSchema.read(config)
        schemaCache.put(c.enclosingRun, s)
        s 
      }
    }

    def toPosition(f: Failure) = {
      val lineOffset = sql.split("\n").take(f.line - 1).map(_.length).sum
      c.enclosingPosition.withPoint(wrappingPos(List(c.prefix.tree)).startOrPoint + f.column + lineOffset)
    }

    def dbConfig = for {
      url      <- sysProp("sqltyped.url")
      driver   <- sysProp("sqltyped.driver")
      username <- sysProp("sqltyped.username")
      password <- sysProp("sqltyped.password")
    } yield DbConfig(url, driver, username, password)

    def generateCode(meta: TypedStatement) =
      codeGen(meta, sql, c, keys, inputsInferred)(config, sqlExpr)

    def fallback = for {
      db   <- dbConfig
      meta <- Jdbc.infer(db, sql)
    } yield meta

    (for {
      db       <- dbConfig
      dialect  = Dialect.choose(db.driver)
      parser   = dialect.parser
      schema   <- cachedSchema(db)
      stmt     <- parse(parser, sql)
      resolved <- Ast.resolveTables(stmt)
      typer    = dialect.typer(schema, resolved)
      typed    <- typer.infer(useInputTags)
      meta     <- new Analyzer(typer).refine(resolved, typed)
    } yield meta) fold (
      fail => fallback fold ( 
        _ => c.abort(toPosition(fail), fail.message), 
        meta => { 
          c.warning(toPosition(fail), "Fallback to JDBC metadata. Please file a bug at https://github.com/jonifreeman/sqltyped/issues")
          generateCode(meta) 
        }
      ),
      meta => generateCode(meta)
    )
  }

  def codeGen[A: c.WeakTypeTag, B: c.WeakTypeTag]
    (meta: TypedStatement, sql: String, c: Context, keys: Boolean, inputsInferred: Boolean)
    (config: c.Expr[Configuration[A, B]], sqlExpr: c.Tree): c.Expr[Any] = {

    import c.universe._
    import MacroSupport._

    def rs(x: TypedValue, pos: Int) = 
      if (x.nullable) {
        Block(
          List(ValDef(Modifiers(), newTermName("x"), TypeTree(), getValue(x, pos))), 
          If(Apply(Select(Ident(newTermName("rs")), newTermName("wasNull")), List()), 
             Select(Ident("scala"), newTermName("None")), 
             Apply(Select(Select(Ident("scala"), newTermName("Some")), newTermName("apply")), List(Ident(newTermName("x"))))))
      } else getValue(x, pos)

    def getValue(x: TypedValue, pos: Int) = {
      def baseValue = Typed(Apply(Select(Ident(newTermName("rs")), newTermName(rsGetterName(x))), 
                                  List(Literal(Constant(pos)))), scalaBaseType(x))
      
      x.tag.map(t =>
        Apply(
          Select(
            TypeApply(
              Select(Select(Ident("shapeless"), newTermName("TypeOperators")), newTermName("tag")), 
              List(tagType(t))), newTermName("apply")), List(baseValue))
      ) getOrElse baseValue
    }

    def scalaBaseType(x: TypedValue) = Ident(c.mirror.staticClass(x.tpe.typeSymbol.fullName))

    def scalaType(x: TypedValue) = {
      x.tag.map(t => 
        AppliedTypeTree(
          Select(Select(Ident("shapeless"), newTermName("TypeOperators")), newTypeName("$at$at")), 
          List(scalaBaseType(x), tagType(t)))
      ) getOrElse scalaBaseType(x)
    }

    def tagType(tag: String) = SelectFromTypeTree(Select(config.tree, "tables"), tag)
    def stmtSetterName(x: TypedValue) = "set" + javaName(x)
    def rsGetterName(x: TypedValue)   = "get" + javaName(x)

    def javaName(x: TypedValue) = 
      if (typeName(x) == "AnyRef" || typeName(x) == "Any") "Object" else typeName(x)

    def typeName(x: TypedValue) = x.tpe.typeSymbol.name.toString

    def setParam(x: TypedValue, pos: Int) =
      if (x.nullable) 
        If(Select(Ident(newTermName("i" + pos)), newTermName("isDefined")), 
           Apply(Select(Ident(newTermName("stmt")), newTermName(stmtSetterName(x))), List(Literal(Constant(pos+1)), Select(Ident(newTermName("i" + pos)), newTermName("get")))), 
           Apply(Select(Ident(newTermName("stmt")), newTermName("setObject")), List(Literal(Constant(pos+1)), Literal(Constant(null)))))
      else
        Apply(Select(Ident(newTermName("stmt")), newTermName(stmtSetterName(x))), 
              List(Literal(Constant(pos+1)), Ident(newTermName("i" + pos))))

    def inputParam(x: TypedValue, pos: Int) = 
      ValDef(Modifiers(Flag.PARAM), newTermName("i" + pos), possiblyOptional(x, scalaType(x)), EmptyTree)

    def inputTypeSig = 
      if (inputsInferred) meta.input.map(col => possiblyOptional(col, scalaType(col)))
      else List(AppliedTypeTree(Ident(c.mirror.staticClass("scala.collection.Seq")), 
                                List(Ident(c.mirror.staticClass("scala.Any")))))

    def possiblyOptional(x: TypedValue, tpe: Tree) = 
      if (x.nullable) AppliedTypeTree(Ident(c.mirror.staticClass("scala.Option")), List(tpe))
      else tpe

    def returnTypeSig = 
      if (meta.output.length == 0) List(Ident(c.mirror.staticClass("scala.Int")))
      else if (meta.output.length == 1) returnTypeSigScalar 
      else returnTypeSigRecord

    def resultTypeSig =
      if (keys && (meta.numOfResults != Many)) scalaType(meta.generatedKeyTypes.head)
      else if (keys) AppliedTypeTree(Ident(newTypeName("List")), List(scalaType(meta.generatedKeyTypes.head)))
      else if (meta.output.length == 0 || meta.numOfResults == One) returnTypeSig.head
      else if (meta.numOfResults == ZeroOrOne) AppliedTypeTree(Ident(newTypeName("Option")), returnTypeSig)
      else AppliedTypeTree(Ident(newTypeName("List")), returnTypeSig)

    def appendRow = 
      if (meta.output.length == 1) appendRowScalar 
      else appendRowRecord

    def returnTypeSigRecord = List(meta.output.foldRight(Ident(c.mirror.staticClass("shapeless.HNil")): Tree) { (x, sig) => 
      AppliedTypeTree(
        Ident(c.mirror.staticClass("shapeless.$colon$colon")), 
        List(AppliedTypeTree(
          Ident(c.mirror.staticClass("scala.Tuple2")), 
          List(recordKeyType(c)(x.name, config.tree), possiblyOptional(x, scalaType(x)))), sig)
      )
    })

    def returnTypeSigScalar = List(possiblyOptional(meta.output.head, scalaType(meta.output.head)))

    def appendRowRecord = 
      mkRecord(c)(
        meta.output, 
        (x: TypedValue, i: Int) => mkTuple2(c)(recordKey(c)(x.name, config.tree), rs(x, meta.output.length - i))) :: Nil

    def appendRowScalar = List(rs(meta.output.head, 1))

    def readRows = 
      List(
        ValDef(
          Modifiers(), newTermName("rows"), TypeTree(), 
          Apply(TypeApply(Select(Select(Select(Select(Ident("scala"), newTermName("collection")), newTermName("mutable")), newTermName("ListBuffer")), newTermName("apply")), returnTypeSig), List())),
        LabelDef(newTermName("while$1"), List(), 
                 If(Apply(Select(Ident(newTermName("rs")), newTermName("next")), List()), 
                    Block(List(Apply(Select(Ident(newTermName("rows")), newTermName("append")), appendRow)), 
                          Apply(Ident(newTermName("while$1")), List())), Literal(Constant(())))))

    val returnRows =
      if (meta.numOfResults == Many)
        Select(Ident(newTermName("rows")), newTermName("toList"))
      else if (meta.numOfResults == ZeroOrOne)
        Select(Select(Ident(newTermName("rows")), newTermName("toList")), newTermName("headOption"))
      else
        Select(Select(Ident(newTermName("rows")), newTermName("toList")), newTermName("head"))

    def processStmt =
      if (meta.isQuery) {
        Apply(
          Apply(Select(Select(Ident("sqltyped"), newTermName("SqlMacro")), newTermName("withResultSet")), List(Ident(newTermName("stmt")))), 
          List(Function(List(ValDef(Modifiers(Flag.PARAM), newTermName("rs"), TypeTree(), EmptyTree)), 
                        Block(readRows, returnRows))))
      } else if (keys && meta.numOfResults == Many) {
        processStmtWithKeys(meta.generatedKeyTypes.head)
      } else if (keys) {
        Select(processStmtWithKeys(meta.generatedKeyTypes.head), newTermName("head"))
      } else {
        Apply(Select(Select(Ident("sqltyped"), newTermName("SqlMacro")), newTermName("withStatement")), List(Ident(newTermName("stmt"))))
      }

    def processStmtWithKeys(keyType: TypedValue) =
      Apply(
        Apply(Select(Select(Ident("sqltyped"), newTermName("SqlMacro")), newTermName("withStatementF")), List(Ident(newTermName("stmt")))),
        List(
          Block(
            List(
              ValDef(Modifiers(), newTermName("rs"), TypeTree(), Apply(Select(Ident(newTermName("stmt")), newTermName("getGeneratedKeys")), List())), 
              ValDef(Modifiers(), newTermName("keys"), TypeTree(), Apply(
                TypeApply(Select(Select(Select(Select(Ident("scala"), newTermName("collection")), newTermName("mutable")), newTermName("ListBuffer")), newTermName("apply")), List(scalaType(keyType))), List())), 
              LabelDef(newTermName("while$1"), List(), 
                       If(Apply(Select(Ident(newTermName("rs")), newTermName("next")), List()), 
                          Block(
                            List(
                              Apply(Select(Ident(newTermName("keys")), newTermName("append")), List(getValue(keyType, 1)))), 
                            Apply(Ident(newTermName("while$1")), List())), Literal(Constant(())))), 
              Apply(Select(Ident(newTermName("rs")), newTermName("close")), List())), 
            Select(Ident(newTermName("keys")), newTermName("toList")))))

    def prepareStatement = 
      Apply(
        Select(Ident(newTermName("conn")), newTermName("prepareStatement")), 
        sqlExpr :: (if (keys) List(Literal(Constant(Statement.RETURN_GENERATED_KEYS))) else Nil))

    def queryF = {
      def argList = 
        if (inputsInferred) meta.input.zipWithIndex.map { case (c, i) => inputParam(c, i) }
        else List(ValDef(Modifiers(Flag.PARAM), newTermName("args$"), 
                         AppliedTypeTree(Ident(c.mirror.staticClass("scala.collection.Seq")), 
                                         List(Ident(c.mirror.staticClass("scala.Any")))), 
                         EmptyTree))

      def processArgs =
        if (inputsInferred) meta.input.zipWithIndex.map { case (c, i) => setParam(c, i) }
        else 
          List(Block(
            List(ValDef(Modifiers(Flag.MUTABLE), newTermName("count$"), TypeTree(), Literal(Constant(0)))), 
            LabelDef(
              newTermName("while$1"), 
              List(), 
              If(Apply(Select(Ident(newTermName("count$")), newTermName("$less")), 
                       List(Select(Ident(newTermName("args$")), newTermName("length")))), 
                 Block(
                   List(
                     Block(
                       List(
                         Apply(Select(Ident(newTermName("stmt")), newTermName("setObject")), 
                               List(Apply(Select(Ident(newTermName("count$")), newTermName("$plus")), List(Literal(Constant(1)))), 
                                    Apply(Select(Ident(newTermName("args$")), newTermName("apply")), List(Ident(newTermName("count$"))))))), 
                       Assign(Ident(newTermName("count$")), 
                              Apply(
                                Select(Ident(newTermName("count$")), newTermName("$plus")), 
                                List(Literal(Constant(1))))))), 
                   Apply(Ident(newTermName("while$1")), List())), Literal(Constant(()))))))


      DefDef(
        Modifiers(), newTermName("apply"), List(), 
        List(
          argList,
          List(ValDef(Modifiers(Flag.IMPLICIT | Flag.PARAM), newTermName("conn"), Ident(c.mirror.staticClass("java.sql.Connection")), EmptyTree))), 
        TypeTree(), 
        Block(
          ValDef(Modifiers(), newTermName("stmt"), TypeTree(), prepareStatement) :: processArgs,
          processStmt
        )
      )
    }

    /* Generates following code:
       new Query1[I1, (name.type, String) :: (age.type, Int) :: HNil] { 
         def apply(i1: I1)(implicit conn: Connection) = {
           val stmt = conn.prepareStatement(sql)
           stmt.setInt(1, i1)
           withResultSet(stmt) { rs =>
             val rows = collection.mutable.ListBuffer[(name.type, String) :: (age.type, Int) :: HNil]()
             while (rs.next) {
               rows.append((name -> rs.getString(1)) :: (age -> rs.getInt(2)) :: HNil)
             }
             rows.toList
           }
         }
       }
    */
    val inputLen = if (inputsInferred) meta.input.length else 1

    c.Expr {
      Block(
        List(
          ClassDef(Modifiers(Flag.FINAL), newTypeName("$anon"), List(), 
                   Template(List(
                     AppliedTypeTree(
                       Ident(c.mirror.staticClass("sqltyped.Query" + inputLen)), inputTypeSig ::: List(resultTypeSig))), 
                            emptyValDef, List(
                              DefDef(
                                Modifiers(), 
                                nme.CONSTRUCTOR, 
                                List(), 
                                List(List()), 
                                TypeTree(), 
                                Block(
                                  List(
                                    Apply(
                                      Select(Super(This(""), ""), nme.CONSTRUCTOR), Nil)), 
                                  Literal(Constant(())))), queryF))
                 )),
        Apply(Select(New(Ident(newTypeName("$anon"))), nme.CONSTRUCTOR), List())
      )
    }
  }
}

// FIXME cleanup by moving Context parameter 
object MacroSupport {
  def mkTuple2(c: Context)(a: c.universe.Tree, b: c.universe.Tree) = {
    import c.universe._
    
    Apply(Select(Apply(
      Select(Ident(c.mirror.staticModule("scala.Predef")), newTermName("any2ArrowAssoc")),
      List(a)), newTermName("->").encoded), List(b))
  }

  def recordKey(c: Context)(name: String, config: c.universe.Tree) = {
    import c.universe._
    
    if (c.typeCheck(Select(Select(config, "columns"), name), silent = true) == EmptyTree)
      Literal(Constant(name))
    else Select(Select(config, "columns"), name)
  }

  def recordKeyType(c: Context)(name: String, config: c.universe.Tree) = {
    import c.universe._

    if (c.typeCheck(Select(Select(config, "columns"), name), silent = true) == EmptyTree)
      Ident(newTypeName("String"))
    else SingletonTypeTree(Select(Select(config, "columns"), name))
  }

  def mkRecord[A](c: Context)(xs: Seq[A], fieldBuilder: (A, Int) => c.universe.Tree) = {
    import c.universe._

    def mkFieldVal(a: A, i: Int) = 
      ValDef(Modifiers(/*Flag.SYNTHETIC*/), 
             newTermName("x$" + (i+1)), 
             TypeTree(), 
             fieldBuilder(a, i))

    val init: Tree = 
      Block(List(
        mkFieldVal(xs.last, 0)), 
            Apply(
              Select(Select(Ident("shapeless"), newTermName("HNil")), newTermName("$colon$colon")),
              List(Ident(newTermName("x$1")))
            ))

    xs.reverse.drop(1).zipWithIndex.foldLeft(init) { case (ast, (x, i)) =>
      Block(
        mkFieldVal(x, i+1),
        Apply(
          Select(
            Apply(
              Select(Ident(c.mirror.staticModule("shapeless.HList")), newTermName("hlistOps")),
              List(Block(ast))
            ), 
            newTermName("$colon$colon")), List(Ident(newTermName("x$" + (i+2))))))
      }
  }
}

// FIXME Replace all these with 'trait SqlF[R]' once Scala macros can create public members
// (apply must be public)
trait Query0[R] { 
  def apply()(implicit conn: Connection): R
}
trait Query1[I1, R] { 
  def apply(i1: I1)(implicit conn: Connection): R
}
trait Query2[I1, I2, R] { 
  def apply(i1: I1, i2: I2)(implicit conn: Connection): R
}
trait Query3[I1, I2, I3, R] { 
  def apply(i1: I1, i2: I2, i3: I3)(implicit conn: Connection): R
}
trait Query4[I1, I2, I3, I4, R] { 
  def apply(i1: I1, i2: I2, i3: I3, i4: I4)(implicit conn: Connection): R
}
trait Query5[I1, I2, I3, I4, I5, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5)(implicit conn: Connection): R
}
trait Query6[I1, I2, I3, I4, I5, I6, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6)(implicit conn: Connection): R
}
trait Query7[I1, I2, I3, I4, I5, I6, I7, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7)(implicit conn: Connection): R
}
trait Query8[I1, I2, I3, I4, I5, I6, I7, I8, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8)(implicit conn: Connection): R
}
trait Query9[I1, I2, I3, I4, I5, I6, I7, I8, I9, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9)(implicit conn: Connection): R
}
trait Query10[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10)(implicit conn: Connection): R
}
trait Query11[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11)(implicit conn: Connection): R
}
trait Query12[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12)(implicit conn: Connection): R
}
trait Query13[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13)(implicit conn: Connection): R
}
trait Query14[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13, i14: I14)(implicit conn: Connection): R
}
trait Query15[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, I15, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13, i14: I14, i15: I15)(implicit conn: Connection): R
}
trait Query16[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, I15, I16, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13, i14: I14, i15: I15, i16: I16)(implicit conn: Connection): R
}
trait Query17[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, I15, I16, I17, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13, i14: I14, i15: I15, i16: I16, i17: I17)(implicit conn: Connection): R
}
trait Query18[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, I15, I16, I17, I18, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13, i14: I14, i15: I15, i16: I16, i17: I17, i18: I18)(implicit conn: Connection): R
}
trait Query19[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, I15, I16, I17, I18, I19, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13, i14: I14, i15: I15, i16: I16, i17: I17, i18: I18, i19: I19)(implicit conn: Connection): R
}
trait Query20[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, I15, I16, I17, I18, I19, I20, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13, i14: I14, i15: I15, i16: I16, i17: I17, i18: I18, i19: I19, i20: I20)(implicit conn: Connection): R
}
trait Query21[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, I15, I16, I17, I18, I19, I20, I21, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13, i14: I14, i15: I15, i16: I16, i17: I17, i18: I18, i19: I19, i20: I20, i21: I21)(implicit conn: Connection): R
}
trait Query22[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, I15, I16, I17, I18, I19, I20, I21, I22, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13, i14: I14, i15: I15, i16: I16, i17: I17, i18: I18, i19: I19, i20: I20, i21: I21, i22: I22)(implicit conn: Connection): R
}
