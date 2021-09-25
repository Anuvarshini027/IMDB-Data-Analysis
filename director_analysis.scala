val movieRdd = sc.textFile("movie_metadata.csv")
val header = movieRdd.first()
val movieRddFilter = movieRdd.filter(r => r!=header)

// director

val director_data = movieRddFilter.map{l => val s = l.split(',')
val(dn,budg,act1n,imdb,nvr) = (s(1),s(22).toDouble,s(10),s(25).toFloat,s(12).toDouble)
(dn,budg,act1n,imdb,nvr)}

director_data.toDF.show()

val avg_nvr = director_data.map{l => l._5}.mean()


// a)Avg imdb rating
val avg_nvr = director_data.map{l => l._5}.mean()
val director_dataF = director_data.filter{l => l._1 !="0" && l._4 !=0 && l._5 >= avg_nvr}
val dir_r = director_dataF.map{case(dn,budg,act1n,imdb,nvr) => (dn,(imdb,nvr,1))}
val dir_rat = dir_r.reduceByKey{case ((pr1,prr1,count1),(pr2,prr2,count2)) => 
(pr1+pr2,prr1+prr2,count1+count2)}.map{l => (l._1,l._2._1/l._2._3,l._2._2/l._2._3)}

val high_rat = dir_rat.sortBy(-_._2).take(20)
val highRdd=sc.parallelize(high_rat)

val directorRdd = highRdd.map{l => (l._1,l._2,l._3)}
directorRdd.toDF.show()

/*val file = new java.io.PrintStream("directorAnalysis.csv")
directorRdd.collect.foreach{file.println(_)}
file.close*/

//val dir_rat = dir_r.reduceByKey{case ((pr1,prr1,count1),(pr2,prr2,count2)) => (pr1+pr2,prr1+prr2,count1+count2)}.mapValues{case (pr,prr,count) => (pr/count,prr/count)}
//val dir_rat = dir_r.map{l => (l._1,l._2._1/l._2._3,l._2._2/l._2._3)}.reduceByKey(_+_)

