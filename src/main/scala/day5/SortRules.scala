package day5

object SortRules {

  implicit object OrderingUser04 extends Ordering[User04] {
    override def compare(x: User04, y: User04): Int = {
      if (x.fv == y.fv) {
        x.age - y.age
      } else {
        y.fv - x.fv
      }
    }
  }

}
