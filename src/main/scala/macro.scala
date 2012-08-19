package sqltyped

import java.sql._

case class Configuration[A](columns: A)

object SqlMacro {
  import shapeless._
  import scala.reflect.makro._

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

    def rs(expr: TypedExpr, pos: Int) = 
      if (expr.nullable) {
        Block(
          List(ValDef(Modifiers(), newTermName("x"), TypeTree(), getValue(expr, pos))), 
          If(Apply(Select(Ident(newTermName("rs")), newTermName("wasNull")), List()), 
             Select(Ident("scala"), newTermName("None")), 
             Apply(Select(Select(Ident("scala"), newTermName("Some")), newTermName("apply")), List(Ident(newTermName("x"))))))
      } else getValue(expr, pos)

    def getValue(expr: TypedExpr, pos: Int) =
        Apply(Select(Ident(newTermName("rs")), newTermName(rsGetterName(expr))), List(Literal(Constant(pos))))

    def scalaType(expr: TypedExpr) = Ident(c.mirror.staticClass(expr.tpe.typeSymbol.fullName))
    def colKey(name: String) = Select(Select(config.tree, "columns"), name)
    def stmtSetterName(expr: TypedExpr) = "set" + expr.tpe.typeSymbol.name
    def rsGetterName(expr: TypedExpr)   = "get" + expr.tpe.typeSymbol.name

    def setParam(expr: TypedExpr, pos: Int) = 
      Apply(Select(Ident(newTermName("stmt")), newTermName(stmtSetterName(expr))), 
            List(Literal(Constant(pos+1)), Ident(newTermName("i" + pos))))

    def inputParam(expr: TypedExpr, pos: Int) = 
      ValDef(Modifiers(Flag.PARAM), newTermName("i" + pos), scalaType(expr), EmptyTree)

    def inputTypeSig = meta.input.map(col => scalaType(col))

    def possiblyOptional(expr: TypedExpr, tpe: Tree) = 
      if (expr.nullable) AppliedTypeTree(Ident(c.mirror.staticClass("scala.Option")), List(tpe))
      else tpe

    val returnTypeSig = List(meta.output.foldRight(Ident(c.mirror.staticClass("shapeless.HNil")): Tree) { (expr, sig) => 
      AppliedTypeTree(
        Ident(c.mirror.staticClass("shapeless.$colon$colon")), 
        List(AppliedTypeTree(
          Ident(c.mirror.staticClass("scala.Tuple2")), 
          List(SingletonTypeTree(colKey(expr.name)), 
               possiblyOptional(expr, scalaType(expr)))), sig)
      )
    })

    val appendRow = {
      def processRow(expr: TypedExpr, i: Int): Tree = 
        ValDef(Modifiers(/*Flag.SYNTHETIC*/), 
               newTermName("x$" + (i+1)), 
               TypeTree(), 
               Apply(Select(Apply(
                 Select(Ident(c.mirror.staticModule("scala.Predef")), newTermName("any2ArrowAssoc")),
                 List(colKey(expr.name))), newTermName("$minus$greater")), List(rs(expr, meta.output.length - i))))

      val init: Tree = 
        Block(List(
          processRow(meta.output.last, 0)), 
          Apply(
            Select(Select(Ident("shapeless"), newTermName("HNil")), newTermName("$colon$colon")),
            List(Ident(newTermName("x$1")))
        ))

      List(meta.output.reverse.drop(1).zipWithIndex.foldLeft(init) { case (ast, (expr, i)) =>
        Block(
          processRow(expr, i+1),
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
          meta.input.zipWithIndex.map { case (c, i) => inputParam(c, i) },
          List(ValDef(Modifiers(Flag.IMPLICIT | Flag.PARAM), newTermName("conn"), Ident(c.mirror.staticClass("java.sql.Connection")), EmptyTree))), 
        AppliedTypeTree(Ident(newTypeName("List")), returnTypeSig), 
        Block(
            ValDef(Modifiers(), newTermName("stmt"), TypeTree(), 
                   Apply(
                     Select(Ident(newTermName("conn")), newTermName("prepareStatement")), 
                     List(Literal(Constant(sql))))) :: meta.input.zipWithIndex.map { case (c, i) => setParam(c, i) },
          Apply(
            Apply(Select(Select(Ident("sqltyped"), newTermName("SqlMacro")), newTermName("withResultSet")), List(Ident(newTermName("stmt")))), 
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
       new Query1[I1, (name.type, String) :: (age.type, Int) :: HNil] { 
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
        List(
          ClassDef(Modifiers(Flag.FINAL), newTypeName("$anon"), List(), 
                   Template(List(
                     AppliedTypeTree(
                       Ident(c.mirror.staticClass("sqltyped.Query" + meta.input.length)), inputTypeSig ::: returnTypeSig)), 
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

trait Query0[R] { 
  def apply()(implicit conn: Connection): List[R]
}
trait Query1[I1, R] { 
  def apply(i1: I1)(implicit conn: Connection): List[R]
}
trait Query2[I1, I2, R] { 
  def apply(i1: I1, i2: I2)(implicit conn: Connection): List[R]
}
trait Query3[I1, I2, I3, R] { 
  def apply(i1: I1, i2: I2, i3: I3)(implicit conn: Connection): List[R]
}
trait Query4[I1, I2, I3, I4, R] { 
  def apply(i1: I1, i2: I2, i3: I3, i4: I4)(implicit conn: Connection): List[R]
}
trait Query5[I1, I2, I3, I4, I5, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5)(implicit conn: Connection): List[R]
}
trait Query6[I1, I2, I3, I4, I5, I6, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6)(implicit conn: Connection): List[R]
}
trait Query7[I1, I2, I3, I4, I5, I6, I7, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7)(implicit conn: Connection): List[R]
}
trait Query8[I1, I2, I3, I4, I5, I6, I7, I8, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8)(implicit conn: Connection): List[R]
}
trait Query9[I1, I2, I3, I4, I5, I6, I7, I8, I9, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9)(implicit conn: Connection): List[R]
}
trait Query10[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10)(implicit conn: Connection): List[R]
}
trait Query11[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11)(implicit conn: Connection): List[R]
}
trait Query12[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12)(implicit conn: Connection): List[R]
}
trait Query13[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13)(implicit conn: Connection): List[R]
}
trait Query14[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13, i14: I14)(implicit conn: Connection): List[R]
}
trait Query15[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, I15, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13, i14: I14, i15: I15)(implicit conn: Connection): List[R]
}
trait Query16[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, I15, I16, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13, i14: I14, i15: I15, i16: I16)(implicit conn: Connection): List[R]
}
trait Query17[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, I15, I16, I17, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13, i14: I14, i15: I15, i16: I16, i17: I17)(implicit conn: Connection): List[R]
}
trait Query18[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, I15, I16, I17, I18, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13, i14: I14, i15: I15, i16: I16, i17: I17, i18: I18)(implicit conn: Connection): List[R]
}
trait Query19[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, I15, I16, I17, I18, I19, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13, i14: I14, i15: I15, i16: I16, i17: I17, i18: I18, i19: I19)(implicit conn: Connection): List[R]
}
trait Query20[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, I15, I16, I17, I18, I19, I20, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13, i14: I14, i15: I15, i16: I16, i17: I17, i18: I18, i19: I19, i20: I20)(implicit conn: Connection): List[R]
}
trait Query21[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, I15, I16, I17, I18, I19, I20, I21, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13, i14: I14, i15: I15, i16: I16, i17: I17, i18: I18, i19: I19, i20: I20, i21: I21)(implicit conn: Connection): List[R]
}
trait Query22[I1, I2, I3, I4, I5, I6, I7, I8, I9, I10, I11, I12, I13, I14, I15, I16, I17, I18, I19, I20, I21, I22, R] {
  def apply(i1: I1, i2: I2, i3: I3, i4: I4, i5: I5, i6: I6, i7: I7, i8: I8, i9: I9, i10: I10, i11: I11, i12: I12, i13: I13, i14: I14, i15: I15, i16: I16, i17: I17, i18: I18, i19: I19, i20: I20, i21: I21, i22: I22)(implicit conn: Connection): List[R]
}
