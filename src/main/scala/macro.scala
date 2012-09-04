package sqltyped

import java.sql._

case class Configuration[A, B](tables: A, columns: B)

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

  def withStatement(stmt: PreparedStatement) = 
    try {
      stmt.executeUpdate
    } finally {
      stmt.close
    }

  def sqlImpl[A: c.TypeTag, B: c.TypeTag](c: Context)(s: c.Expr[String])(config: c.Expr[Configuration[A, B]]): c.Expr[Any] = {
    import c.universe._

    val Literal(Constant(sql: String)) = s.tree
    
    val url = System.getProperty("sqltyped.url")
    val driver = System.getProperty("sqltyped.driver")
    val username = System.getProperty("sqltyped.username")
    val password = System.getProperty("sqltyped.password")

    val stmt = SqlParser.parse(sql) fold (
      err => sys.error("Parse failed: " + err),
      res => res
    )
    val meta = Analyzer.refine(Typer.infer(stmt.resolveTables, url, driver, username, password))

    def rs(x: TypedValue, pos: Int) = 
      if (x.nullable) {
        Block(
          List(ValDef(Modifiers(), newTermName("x"), TypeTree(), getValue(x, pos))), 
          If(Apply(Select(Ident(newTermName("rs")), newTermName("wasNull")), List()), 
             Select(Ident("scala"), newTermName("None")), 
             Apply(Select(Select(Ident("scala"), newTermName("Some")), newTermName("apply")), List(Ident(newTermName("x"))))))
      } else getValue(x, pos)

    def getValue(x: TypedValue, pos: Int) = {
      def baseValue = Apply(Select(Ident(newTermName("rs")), newTermName(rsGetterName(x))), 
                            List(Literal(Constant(pos))))
      
      x.tag.map(t =>
        Apply(
          Select(
            TypeApply(
              Select(Select(Ident("shapeless"), newTermName("TypeOperators")), newTermName("tag")), 
              List(tagType(t))), newTermName("apply")), List(baseValue))
      ) getOrElse baseValue
    }

    def scalaType(x: TypedValue) = {
      def baseType = Ident(c.mirror.staticClass(x.tpe.typeSymbol.fullName))

      x.tag.map(t => 
        AppliedTypeTree(
          Select(Select(Ident("shapeless"), newTermName("TypeOperators")), newTypeName("$at$at")), 
          List(baseType, tagType(t)))
      ) getOrElse baseType
    }

    def colKey(name: String) = Select(Select(config.tree, "columns"), name)
    def tagType(tag: String) = SelectFromTypeTree(Select(config.tree, "tables"), tag)
    def stmtSetterName(x: TypedValue) = "set" + javaName(x)
    def rsGetterName(x: TypedValue)   = "get" + javaName(x)

    def javaName(x: TypedValue) = 
      if (x.tpe.typeSymbol.name.toString == "AnyRef") "Object" else x.tpe.typeSymbol.name.toString

    def setParam(x: TypedValue, pos: Int) = 
      Apply(Select(Ident(newTermName("stmt")), newTermName(stmtSetterName(x))), 
            List(Literal(Constant(pos+1)), Ident(newTermName("i" + pos))))

    def inputParam(x: TypedValue, pos: Int) = 
      ValDef(Modifiers(Flag.PARAM), newTermName("i" + pos), scalaType(x), EmptyTree)

    def inputTypeSig = meta.input.map(col => scalaType(col))

    def possiblyOptional(x: TypedValue, tpe: Tree) = 
      if (x.nullable) AppliedTypeTree(Ident(c.mirror.staticClass("scala.Option")), List(tpe))
      else tpe

    def returnTypeSig = 
      if (meta.output.length == 0) List(Ident(c.mirror.staticClass("scala.Int")))
      else if (meta.output.length == 1) returnTypeSigScalar 
      else returnTypeSigRecord

    def resultTypeSig =
      if (meta.output.length == 0) returnTypeSig.head
      else AppliedTypeTree(Ident(newTypeName(if (meta.multipleResults) "List" else "Option")), returnTypeSig)

    def appendRow = 
      if (meta.output.length == 1) appendRowScalar 
      else appendRowRecord

    def returnTypeSigRecord = List(meta.output.foldRight(Ident(c.mirror.staticClass("shapeless.HNil")): Tree) { (x, sig) => 
      AppliedTypeTree(
        Ident(c.mirror.staticClass("shapeless.$colon$colon")), 
        List(AppliedTypeTree(
          Ident(c.mirror.staticClass("scala.Tuple2")), 
          List(SingletonTypeTree(colKey(x.name)), 
               possiblyOptional(x, scalaType(x)))), sig)
      )
    })

    def returnTypeSigScalar = List(possiblyOptional(meta.output.head, scalaType(meta.output.head)))

    def appendRowRecord = {
      def processRow(x: TypedValue, i: Int): Tree = 
        ValDef(Modifiers(/*Flag.SYNTHETIC*/), 
               newTermName("x$" + (i+1)), 
               TypeTree(), 
               Apply(Select(Apply(
                 Select(Ident(c.mirror.staticModule("scala.Predef")), newTermName("any2ArrowAssoc")),
                 List(colKey(x.name))), newTermName("$minus$greater")), List(rs(x, meta.output.length - i))))

      val init: Tree = 
        Block(List(
          processRow(meta.output.last, 0)), 
          Apply(
            Select(Select(Ident("shapeless"), newTermName("HNil")), newTermName("$colon$colon")),
            List(Ident(newTermName("x$1")))
        ))

      List(meta.output.reverse.drop(1).zipWithIndex.foldLeft(init) { case (ast, (x, i)) =>
        Block(
          processRow(x, i+1),
          Apply(
            Select(
              Apply(
                Select(Ident(c.mirror.staticModule("shapeless.HList")), newTermName("hlistOps")),
                List(Block(ast))
              ), 
              newTermName("$colon$colon")), List(Ident(newTermName("x$" + (i+2))))))
         })
    }

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
      if (meta.multipleResults)
        Select(Ident(newTermName("rows")), newTermName("toList"))
      else
        Select(Select(Ident(newTermName("rows")), newTermName("toList")), newTermName("headOption"))

    def processStmt =
      if (meta.stmt.isQuery) {
        Apply(
          Apply(Select(Select(Ident("sqltyped"), newTermName("SqlMacro")), newTermName("withResultSet")), List(Ident(newTermName("stmt")))), 
          List(Function(List(ValDef(Modifiers(Flag.PARAM), newTermName("rs"), TypeTree(), EmptyTree)), 
                        Block(readRows, returnRows))))
      } else {
        Apply(Select(Select(Ident("sqltyped"), newTermName("SqlMacro")), newTermName("withStatement")), List(Ident(newTermName("stmt"))))
      }

    val queryF = 
      DefDef(
        Modifiers(), newTermName("apply"), List(), 
        List(
          meta.input.zipWithIndex.map { case (c, i) => inputParam(c, i) },
          List(ValDef(Modifiers(Flag.IMPLICIT | Flag.PARAM), newTermName("conn"), Ident(c.mirror.staticClass("java.sql.Connection")), EmptyTree))), 
        TypeTree() /*AppliedTypeTree(Ident(newTypeName("List")), returnTypeSig)*/, 
        Block(
          ValDef(Modifiers(), newTermName("stmt"), TypeTree(), 
                 Apply(
                   Select(Ident(newTermName("conn")), newTermName("prepareStatement")), 
                   List(Literal(Constant(sql))))) :: meta.input.zipWithIndex.map { case (c, i) => setParam(c, i) },
          processStmt
        )
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
                       Ident(c.mirror.staticClass("sqltyped.Query" + meta.input.length)), inputTypeSig ::: List(resultTypeSig))), 
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
