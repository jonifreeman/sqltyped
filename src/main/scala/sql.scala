package sqltyped

case class Configuration[A](columns: A)

object Sql {
  import java.sql._
  import shapeless._
  import scala.reflect.makro._

  // FIXME add more these
  trait QueryF0[R] { 
    def apply()(implicit conn: Connection): List[R]
  }
  trait QueryF1[I1, R] { 
    def apply(i1: I1)(implicit conn: Connection): List[R]
  }
  trait QueryF2[I1, I2, R] { 
    def apply(i1: I1, i2: I2)(implicit conn: Connection): List[R]
  }
  trait QueryF3[I1, I2, I3, R] { 
    def apply(i1: I1, i2: I2, i3: I3)(implicit conn: Connection): List[R]
  }
  trait QueryF4[I1, I2, I3, I4, R] { 
    def apply(i1: I1, i2: I2, i3: I3, i4: I4)(implicit conn: Connection): List[R]
  }

  case class Query1[C1, R1]
  case class Query2[C1, R1, C2, R2]
  case class Query3[C1, R1, C2, R2, C3, R3]
  case class Query4[C1, R1, C2, R2, C3, R3, C4, R4]
  case class Query5[C1, R1, C2, R2, C3, R3, C4, R4, C5, R5]
  case class Query6[C1, R1, C2, R2, C3, R3, C4, R4, C5, R5, C6, R6]

  def withResultSet[A](stmt: PreparedStatement)(f: ResultSet => A) = {
    var rs: ResultSet = null
    try {
      f(stmt.executeQuery)
    } finally {
      if (rs != null) try rs.close catch { case e: Exception => }
      stmt.close
    }
  }

  def sqlImpl[A: c.TypeTag](c: Context)(s: c.Expr[String])(config: c.Expr[Configuration[A]]): c.Expr[Any] = {
    import c.universe._

    val Literal(Constant(sql: String)) = s.tree
    
    val url = System.getProperty("sqltyped.url")
    val driver = System.getProperty("sqltyped.driver")
    val username = System.getProperty("sqltyped.username")
    val password = System.getProperty("sqltyped.password")

    val select = SqlParser.parse(sql) fold (
      err => sys.error("Parse failed: " + err),
      res => res
    )
    val meta = Schema.infer(select, url, driver, username, password)

    def rs(name: String, pos: Int) = 
      Apply(Select(Ident(newTermName("rs")), newTermName(name)), List(Literal(Constant(pos))))

    def scalaType(col: TypedColumn) = Ident(c.mirror.staticClass(col.tpe.typeSymbol.fullName))
    def col(name: String) = Select(Select(config.tree, "columns"), name)
    def rsGetterName(c: TypedColumn)   = "get" + c.tpe.typeSymbol.name
    def stmtSetterName(c: TypedColumn) = "set" + c.tpe.typeSymbol.name

    def setParam(c: TypedColumn, pos: Int) = 
      Apply(Select(Ident(newTermName("stmt")), newTermName(stmtSetterName(c))), 
            List(Literal(Constant(pos+1)), Ident(newTermName("i" + pos))))

    def queryParam(c: TypedColumn, pos: Int) = 
      ValDef(Modifiers(Flag.PARAM), newTermName("i" + pos), scalaType(c), EmptyTree)

    def queryFTypeSig = meta.input.map(c => scalaType(c))

    val typeSig = meta.columns.flatMap { c => 
      List(SingletonTypeTree(col(c.column.name)), scalaType(c)) 
    }

    val returnTypeSig = List(meta.columns.foldRight(Ident(c.mirror.staticClass("shapeless.HNil")): Tree) { (column, sig) => 
      AppliedTypeTree(
        Ident(c.mirror.staticClass("shapeless.$colon$colon")), 
        List(AppliedTypeTree(Ident(c.mirror.staticClass("scala.Tuple2")), 
                             List(SingletonTypeTree(col(column.column.name)), scalaType(column))), sig)
      )
    })

    val appendRow = {
      def processRow(column: TypedColumn, i: Int): Tree = 
        ValDef(Modifiers(/*Flag.SYNTHETIC*/), 
               newTermName("x$" + (i+1)), 
               TypeTree(), 
               Apply(Select(Apply(
                 Select(Ident(c.mirror.staticModule("scala.Predef")), newTermName("any2ArrowAssoc")),
                 List(col(column.column.name))), newTermName("$minus$greater")), List(rs(rsGetterName(column), meta.columns.length - i))))

      val init: Tree = 
        Block(List(
          processRow(meta.columns.last, 0)), 
          Apply(
            Select(Select(Ident("shapeless"), newTermName("HNil")), newTermName("$colon$colon")),
            List(Ident(newTermName("x$1")))
        ))

      List(meta.columns.reverse.drop(1).zipWithIndex.foldLeft(init) { case (ast, (column, i)) =>
        Block(
          processRow(column, i+1),
          Apply(
            Select(
              Apply(
                Select(Ident(c.mirror.staticModule("shapeless.HList")), newTermName("hlistOps")),
                List(Block(ast))
              ), 
              newTermName("$colon$colon")), List(Ident(newTermName("x$" + (i+2))))))
         })
    }

    // FIXME cleanup code generation
    val queryF = 
      DefDef(
        Modifiers(), newTermName("apply"), List(), 
        List(
          meta.input.zipWithIndex.map { case (c, i) => queryParam(c, i) },
          List(ValDef(Modifiers(Flag.IMPLICIT | Flag.PARAM), newTermName("conn"), Ident(c.mirror.staticClass("java.sql.Connection")), EmptyTree))), 
        AppliedTypeTree(Ident(newTypeName("List")), returnTypeSig), 
        Block(
            ValDef(Modifiers(), newTermName("stmt"), TypeTree(), 
                   Apply(
                     Select(Ident(newTermName("conn")), newTermName("prepareStatement")), 
                     List(Literal(Constant(sql))))) :: meta.input.zipWithIndex.map { case (c, i) => setParam(c, i) },
          Apply(
            Apply(Select(Select(Ident("sqltyped"), newTermName("Sql")), newTermName("withResultSet")), List(Ident(newTermName("stmt")))), 
            List(Function(List(ValDef(Modifiers(Flag.PARAM), newTermName("rs"), TypeTree(), EmptyTree)), 
                          Block(List(ValDef(Modifiers(), newTermName("rows"), TypeTree(), 
                                            Apply(
                                              TypeApply(
                                                Select(Select(Select(Select(Ident("scala"), newTermName("collection")), newTermName("mutable")), newTermName("ListBuffer")), 
                                                       newTermName("apply")), returnTypeSig), List())), 
                                     LabelDef(newTermName("while$1"), List(), 
                                              If(Apply(Select(Ident(newTermName("rs")), newTermName("next")), List()), 
                                                 Block(List(Apply(Select(Ident(newTermName("rows")), newTermName("append")), appendRow)), 
                                                       Apply(Ident(newTermName("while$1")), List())), Literal(Constant(()))))), 
                                Select(Ident(newTermName("rows")), newTermName("toList")))))))
      )

    /* Generates following code:
       new Query2(sql, name, rs => rs.getString(1), age, rs => rs.getInt(2)) with trait Query1F[I1, (name.type, String) :: (age.type, Int) :: HNil] { 
         def apply(i1: I1)(implicit conn: Connection) = {
           val stmt = conn.prepareStatement(sql)
           stmt.setInt(i1)
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
    c.Expr {
      Block(
        List(ClassDef(Modifiers(Flag.FINAL), newTypeName("$anon"), List(), 
                      Template(List(
                        AppliedTypeTree(Ident(c.mirror.staticClass("sqltyped.Sql.Query" + meta.columns.length)), typeSig), 
                        AppliedTypeTree(Ident(c.mirror.staticClass("sqltyped.Sql.QueryF" + meta.input.length)), queryFTypeSig ::: returnTypeSig)), emptyValDef, List(DefDef(Modifiers(), nme.CONSTRUCTOR, List(), List(List()), TypeTree(), Block(List(Apply(Select(Super(This(""), ""), nme.CONSTRUCTOR), Nil)), Literal(Constant(())))), queryF))
                    )),
        Apply(Select(New(Ident(newTypeName("$anon"))), nme.CONSTRUCTOR), List())
      )
    }
  }
}
