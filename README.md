The ouput file int the directory contains the sorted expected output

Description:
In this project we will use Hadoop MapReduce to implement a very basic “Sentiment Analysis” using the review text in the Yelp Academic Dataset as training data. 

The Scoring Algorithm:
For each word that appears in the text of a review, the job will compute a score summarizing the "positivity" or "negativity" of that word.
The score for a word is computed by applying the following rules:
•	Each occurrence in a 5-star review increases the word's score by 2.
•	Each occurrence in a 4-star review increases the word's score by 1.
•	3-star reviews have no impact on word score.
•	Each occurrence in a 2-star review decreases the word's score by 1.
•	Each occurrence in a 1-star review decreases the word's score by 2.

Consider an input dataset containing only the following three reviews:
{"stars": 4, "text": "Good food, great times."}
{"stars": 5, "text": "Been many times. Will be back."}
{"stars": 1, "text": "Rude staff. Sent my tacos back."}

The scores for each of the words are computed as follows:
score("Good") = 1
score("food,") = 1
score("great") = 1
score("times.") = 1 + 2 = 3
score("Been") = 2
score("many") = 2
score("Will") = 2
score("be") = 2
score("back.") = 2 + -2 = 0
score("Rude") = -2
score("staff.") = -2
score("Sent") = -2
score("my") = -2
score("tacos") = -2

The following is one example of a correct final output for the above input:
3    times.
2    be
2    many
2    Will
2    Been
1    great
1    food,
1    Good
0    back.
-2    tacos
-2    Sent
-2    staff.
-2    my
-2    Rude

