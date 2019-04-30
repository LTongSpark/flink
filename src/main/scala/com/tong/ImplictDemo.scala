package com.tong

object ImplictDemo {
    implicit object getInt extends Ordering[Gail]{
        override def compare(x: Gail, y: Gail): Int = if (x.age > y.age) 1 else -1
    }
}

class Gail(val name : String ,val age :Int){
    override def toString: String = s"name : $name ,age : $age"
}

class goss[T:Ordering](val v1 : T ,val v2 : T){
    def choss(implicit ord : Ordering[T]) = if(ord.gt(v1 ,v2)) v1 else v2
}

object goss{
    def main(args: Array[String]): Unit = {
        val g1 = new Gail("tong" ,2)
        val g2 = new Gail("tong" ,4)
        import ImplictDemo.getInt
        print(new goss(g1,g2).choss)
    }
}

