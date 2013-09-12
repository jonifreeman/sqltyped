package sqltyped

private[sqltyped] class Timer(enabled: Boolean) {
  def apply[A](msg: => String, indent: Int, a: => A) = 
    if (enabled) {
      val start = System.currentTimeMillis
      println((" " * indent) + msg)
      val aa = a
      println((" " * indent) + (System.currentTimeMillis - start) + "ms")
      aa
    } else a
}

private[sqltyped] object Timer {
  def apply(enabled: Boolean) = new Timer(enabled)
}
