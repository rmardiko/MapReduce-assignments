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
(cloud, tabernacle) -7.805963504236849
(cloud, glory) -8.56011335651833
(cloud, fire) -8.723516046759277
(love, hate) -9.38345294698951
(love, hermia) -9.929996712342444
(love, commandments) -10.019441737740376
