//IMDB DATASET 
//dataset that contains movies from the year 1916 to 2016

val movieRdd = sc.textFile("movie_metadata.csv")


val head = movieRdd.first()


val movieRddFilter = movieRdd.filter(r => r != head)


val movieSchemeRdd = movieRddFilter.map{l => val str1 = l.split(',')
  val (dn,a2n,gen,a1n,mt,a3n,lan,coun)=(str1(1),str1(6),str1(9),str1(10),str1(11),str1(14),str1(19),str1(20))
  val (ncr,dur,dfl,a3fl,a1fl,gro,nvu,ctfl,bud,ty,a2fl,imdbs,mfl)=(str1(2).toInt,str1(3).toInt,str1(4).toInt,str1(5).toInt,str1(7).toInt,str1(8).toLong,str1(12).toInt,str1(13).toInt,str1(22).toLong,str1(23).toInt,str1(24).toInt,str1(25).toFloat,str1(27).toInt)
  (dn,ncr,dur,dfl,a3fl,a2n,a1fl,gro,gen,a1n,mt,nvu,ctfl,a3n,lan,coun,bud,ty,a2fl,imdbs,mfl)}


movieSchemeRdd.toDF.show(5)

val movieSchemeProfit=movieRddFilter.map{l => val str1 = l.split(',')

  val (dn,a2n,gen,a1n,mt,a3n,lan,coun,content_rating)=(str1(1),str1(6),str1(9),str1(10),str1(11),str1(14),str1(19),str1(20),str1(21))
  val (ncr,dur,dfl,a3fl,a1fl,gro,nvu,ctfl,bud,ty,a2fl,imdbs,mfl)=(str1(2).toInt,str1(3).toInt,str1(4).toInt,str1(5).toInt,str1(7).toInt,str1(8).toLong,str1(12).toInt,str1(13).toInt,str1(22).toLong,str1(23).toInt,str1(24).toInt,str1(25).toFloat,str1(27).toInt)
  (gro,gen,mt,content_rating,bud,ty,imdbs,mfl,coun)}

movieSchemeProfit.toDF.show(5)

println("Profit") //calculating the profit and listing top 10 movies
val movieSchemeProfitF = movieSchemeProfit.filter{l => l._1 !=0 && l._5 !=0 && l._9=="USA"}
val profit = movieSchemeProfitF.map{case(gro,gen,mt,content_rating,bud,ty,imdbs,mfl,coun)=>
 	val pro=(gro/1000000)-(bud/1000000)
    (pro,mt,gen,ty,imdbs,mfl)}.sortBy(-_._1)
profit.toDF("Profit(million $)","movie_title","genres","title_year","imdb_score","movie_facebook_likes").take(20)


println("Movies with negative Profit")
val negativeProfit= profit.filter(l=> l._1<0).sortBy(x => (x._1,false))
negativeProfit.toDF("Negative Profit(million $)","movie_title","genres","title_year","imdb_score","movie_facebook_likes").show()

println("Movies with good IMDB rating but low negativeProfit")
val goodMovWithNegativePro = negativeProfit.filter(l=> l._5 > 8.0).sortBy(-_._5)
goodMovWithNegativePro.toDF("Negative Profit(million $)","movie_title","genres","title_year","imdb_score above 8","movie_facebook_likes").show()
println("We can notice that there are some movies with negative profit. Although good movies do incur losses.")

println("IMDb rating of a movie. This rating is determined by taking the average of hundred-thousands of ratings from the general audience.")
println("IMDB score above 8") // analysing the imdb score to check if high rated movies are giving more profit
val imdbTop = profit.filter(l => l._5 > 8.0).sortBy(-_._5)
imdbTop.toDF("Profit(million $)","movie_title","genres","title_year","imdb_score(above 8)","movie_facebook_likes").show(20)

println("Number of movies each year in each genre") //to analyse the most released genres in each year
val yearFilter = movieSchemeProfitF.filter(l=>l._6!=0)
val yearUniqueCount = yearFilter.map{case(gro,gen,mt,content_rating,bud,ty,imdbs,mfl,coun)=>
val gen1= gen.split('|')
 ((ty,gen1(0)),1)}.reduceByKey(_+_).map{l=> (l._1._1,l._1._2,l._2)}sortBy(_._3)
yearUniqueCount.toDF("year","Genre","Count").show()

println("Count of Genre")
val genreUniqueCount = movieSchemeProfitF.map{case(gro,gen,mt,content_rating,bud,ty,imdbs,mfl,coun)=>
val gen1= gen.split('|')
 ((gen1(0)),1)}.reduceByKey(_+_).sortBy(-_._2)
genreUniqueCount.toDF("different_genre","count").show()

println("R Rated Movies")
val contentR =  movieSchemeProfitF.filter(l=> l._4 == "R").sortBy(-_._7)
contentR.toDF("gross","genres","movie_title","content_rating","budget","title_year","imdb_score","movie_facebook_likes","country").show()

println("IMDB ratings based on critic reviews and voted users")
val review_voted = movieSchemeRdd.filter{l =>l._18 != 0}.map{l=>
(l._2,l._12,l._11,l._18,l._20)}.sortBy(-_._5)
review_voted.toDF("num_critic_for_reviews","num_voted_users","movie_title","title_year","imdb_score").show()


println("Count of different country")
val countryUniqueCount = movieSchemeRdd .map{l =>
 (l._16,1)}.reduceByKey(_+_).sortBy(-_._2)
countryUniqueCount.toDF("different_country","count").show()

println("Count of different language")
val languageUniqueCount = movieSchemeRdd .map{l =>
 (l._15,1)}.reduceByKey(_+_).sortBy(-_._2)
languageUniqueCount.toDF("different_languages","count").show()

println("Highest Profit for each year")
val hpry = profit.map{l => (l._4,l._1)}.reduceByKey(math.max(_, _)).sortBy(-_._1)
hpry.toDF("Year","Highest Profit in that year").show()
