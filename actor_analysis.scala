val movieRdd = sc.textFile("movie_metadata.csv")
val header = movieRdd.first()
val movieRddFilter = movieRdd.filter(r => r!=header)

//actor

val actor_data = movieRddFilter.map{l => val s = l.split(',')
val(act1n,act2n,act3n,imdb,fb,cast,nvr) = (s(10),s(6),s(14),s(25).toFloat,s(7).toDouble,s(13).toDouble,s(12).toDouble)
(act1n,act2n,act3n,imdb,fb,cast,nvr)}

actor_data.toDF.show()

val avg_nvr = actor_data.map{l => l._5}.mean()
val actor_dataF = actor_data.filter{l => l._1 !="0" && l._2 !=0 && l._5 >= avg_nvr}
val act_r = actor_dataF.map{case(act1n,act2n,act3n,imdb,fb,cast,nvr) =>
 (act1n,(imdb,fb,nvr,1))}
val act_rat = act_r.reduceByKey{case ((pr1,prr1,prrr1,count1),(pr2,prr2,prrr2,count2)) => 
(pr1+pr2,prr1+prr2,prrr1+prrr2,count1+count2)}.map{l =>
(l._1,l._2._1/l._2._4,l._2._2/l._2._4,l._2._3/l._2._4)}
val high_rat = act_rat.sortBy(-_._2).take(20)
val highRdd=sc.parallelize(high_rat)
val actorRdd = highRdd.map{l => (l._1,l._2,l._3,l._4)}
actorRdd.toDF.show()
/*val file = new java.io.PrintStream("actorAnalysis.csv")
actorRdd.collect.foreach{file.println(_)}
file.close*/
//act_rat.toDF.show()

val trio =actor_data.filter{l => l._6 !=0}.map{l => ((l._1,l._2,l._3),l._6)}.sortBy(-_._2)
trio.toDF.show()


/*val file = new java.io.PrintStream("trio.csv")
trio.collect.foreach{file.println(_)}
file.close*/
//val dir_rat = dir_r.reduceByKey{case ((pr1,prr1,count1),(pr2,prr2,count2)) => (pr1+pr2,prr1+prr2,count1+count2)}.mapValues{case (pr,prr,count) => (pr/count,prr/count)}
