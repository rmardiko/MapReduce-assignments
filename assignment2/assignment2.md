0. Pairs PMI
My MapReduce solution for PairsPMI consists of two jobs. The first job generates and counts word pairs (word1, word2) and the marginals (word, *). The reduce function calculates "partial" PMI pmi1 = (p(word1,word2)/p(word1)). The map function of the second job switches the word order (word1, word2) -> (word2, word1) and the reduce function calculates the PMI by dividing pmi1/p(word2). The intermediate key value pairs are (PairsOfStrings, FloatWritable) -> (PairsOfStrings, FloatWritable). <p>
Stripes PMI
The solution for StripesPMI also has two jobs. The first job counts word pairs by using hashmap (HMapSIW class). So for each word we have a hashmap [w1:countw1, w2:countw2, ...] that indicates the cooccurence of the word with other words. The reduce function accumulates those hashmaps and create word pairs. The second job is exactly the same as the second job in Pairs PMI. In this solution the intermediate key value pairs are (Text, HMapSIW) -> (PairOfStrings, FloatWritable).

1. PairsPMI: 498 seconds
   StripesPMI: 150 seconds

2. PairsPMI: 547 seconds
   StripesPMI: 119 seconds

3. 233518

4. The pair (abednego, meshach) has the highest PMI. Because the words abednego and meshach frequently appear together in a document, the PMI score is high even if they don't appear many times in the entire dataset. The names abednego and meshach are mentioned in a specific story in the Bible.

5. Pair --  PMI Value
(cloud, tabernacle) 4.072988E-4
(cloud, glory) 1.9159757E-4
(cloud, fire) 1.6271407E-4
(love, hate) 8.410429E-5
(love, hermia) 4.8691956E-5
(love, commandments) 4.45258E-5