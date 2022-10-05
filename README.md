# Apache Spark with Scala

Design and implement applications using Apache Spark.

Data management using Spark SQL and processing text files.
The Scala programming language was used to develop the programs.



- Word-count
By using Spark's RDD API. The program reads a text file, delete all punctuation and convert all characters to
low caps.
The following actions will then take place:
1. Counting the number of word pairs (a word pair consists of two words that
one follows the other on the same line of text)
2. Count the number of pairs of words, but this time take into account
only the pairs where each word they make up are 3 characters long or
more.
3. Print the 5 most frequent pairs of query 2, in descending order as
to the number of appearances. E.g. <word, count>

The above will be applied, with two different executions of the program, in two
different files given: a) Shakespeare.txt and b) SherlockHolmes.txt.

**************************************

- Market basket
Work with shopping cart data. Εach basket is a transaction.

A text file is given where in each line there is a transaction (shopping basket). 
Your goal is to output all dyads and triads of products that have been purchased 
together at least s times, where s is a constant within in the code.
S can pass also as a parameter to main. Run the program using the groceries.csv file.

Example
bread, butter, milk
milk, butter
butter, milk, beans
butter, milk, beans, merenda
milk, beans
wine, chips

If we want all duos and trios that have been bought together at least 2 times, the
output is:
(butter, milk), 4
(milk, beans), 3
(butter, milk, beans), 2



******************

- Movie Country
Analyze an IMDB dataset about movies and draw conclusions which may assist the 
IMDB team in providing movie recommendations by genre and country.
The dataset (movies.zip) includes the following elements:
. imdbID
. Movie title
. Year
. Duration
. Species
. Date of 1st view
. Score
. Total votes
. Country/s

1. What is the average score and number of films per year.
2. For each country, find the film (title and year) with the highest score.
3. Find all pairs of movies (IMDBIDs) whose score difference is less than or equal to 1. 
(Note: We don't mind if a movie with the itself as a pair).

********************


- COVID VAX
A dataset on the course of vaccination against the COVID-19 virus.
The dataset is a CSV file containing the following columns (country_vaccinations_by_manufacturer.csv):
. location
. date
. vaccine
. total_vaccinations

Develop a Spark application in the Scala language:
1. What is the average daily vaccinations a) overall, b) for the year 2021 and
c) for the year 2022?
2. For each location, what is the number of days for which the number of vaccines
is greater than the total average daily vaccinations you calculated
in 1a? Sort the results from largest to smallest.
3. For each location witch is the vaccine used in more times and which the least?