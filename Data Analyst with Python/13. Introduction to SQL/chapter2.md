---
title: Filtering rows
description: >-
  This chapter builds on the first by teaching you how to filter tables for rows
  satisfying some criteria of interest. You'll learn how to use basic comparison
  operators, combine multiple criteria, match patterns in text, and much more.
---

## Filtering results

Congrats on finishing the first chapter! You now know how to select columns and perform basic counts. This chapter will focus on filtering your results.

In SQL, the `WHERE` keyword allows you to filter based on both text and numeric values in a table. There are a few different comparison operators you can use:

- `=` equal
- `<>` not equal
- `<` less than
- `>` greater than
- `<=` less than or equal to
- `>=` greater than or equal to

For example, you can filter text records such as `title`. The following code returns all films with the title `'Metropolis'`:

```
SELECT title
FROM films
WHERE title = 'Metropolis';
```

Notice that the `WHERE` clause always comes after the `FROM` statement!

**Note that in this course we will use `<>` and not `!=` for the not equal operator, as per the SQL standard.**

<hr>
What does the following query return?

```
SELECT title
FROM films
WHERE release_year > 2000;
```

`@hint`
If you're stuck, refer to the list of comparison operators above!

`@possible_answers`
- Films released before the year 2000
- [Films released after the year 2000]
- Films released after the year 2001
- Films released in 2000

`@feedback`
- Incorrect. `>` means *strictly* greater than and *not* equal to.
- Correct!
- Incorrect. `>` means *strictly* greater than and *not* equal to.
- Incorrect. `>` means *strictly* greater than and *not* equal to.

---

## Simple filtering of numeric values

As you learned in the previous exercise, the `WHERE` clause can also be used to filter numeric records, such as years or ages.

For example, the following query selects all details for films with a budget over ten thousand dollars:

```
SELECT *
FROM films
WHERE budget > 10000;
```

Now it's your turn to use the `WHERE` clause to filter numeric values!

`@pre_exercise_code`
```{python}
connect('postgresql', 'films')
set_options(visible_tables = ['films'])
```

***

`@instructions`
Get all details for all films released in 2016.

`@hint`
```
SELECT ___
FROM ___
WHERE ___ = ___;
```

`@solution`
```{sql}
SELECT *
FROM films
WHERE release_year = 2016;
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').has_equal_ast()
    )
)

# Now, check if all columns are correct
Ex().check_correct(
    check_all_columns().has_equal_value(),
    check_node('SelectStmt').check_node('Star', missing_msg="Are you using `SELECT *` to select _all_ columns?")
)
```

***

`@instructions`
Get the number of films released before 2000.

`@hint`
```
SELECT ___(*)
FROM ___
WHERE ___ < ___;
```

`@solution`
```{sql}
SELECT COUNT(*)
FROM films
WHERE release_year < 2000;
```

`@sct`
```{python}
cnt_msg = "Are you using `COUNT(*)`?"
Ex().check_correct(
    check_column('count').has_equal_value(),
    check_node('SelectStmt').multi(
        check_node('Call', missing_msg=cnt_msg).check_edge('name', missing_msg=cnt_msg),
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').has_equal_ast()
    )
)
```

***

`@instructions`
Get the title and release year of films released after 2000.

`@hint`
```
SELECT ___, ___
FROM ___
WHERE ___ > ___;
```

`@solution`
```{sql}
SELECT title, release_year
FROM films
WHERE release_year > 2000;
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').has_equal_ast()
    )
)

# Next check if right columns were included
check_all_columns().has_equal_value()

Ex().success_msg("Great job! After filtering of numeric values, it's time to explore filtering of text!")
```

---

## Simple filtering of text

Remember, the `WHERE` clause can also be used to filter text results, such as names or countries.

For example, this query gets the titles of all films which were filmed in China:

```
SELECT title
FROM films
WHERE country = 'China';
```

Now it's your turn to practice using `WHERE` with text values!

**Important: in PostgreSQL (the version of SQL we're using), you must use single quotes with `WHERE`.**

`@pre_exercise_code`
```{python}
connect('postgresql', 'films')
set_options(visible_tables = ['films', 'people'])
```

***

`@instructions`
Get all details for all French language films.

`@hint`
```
SELECT ___
FROM ___
WHERE ___ = '___';
```

`@solution`
```{sql}
SELECT *
FROM films
WHERE language = 'French';
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').has_equal_ast()
    )
)

# Next check if right columns were included
Ex().check_correct(
    check_all_columns().has_equal_value(),
    check_node('SelectStmt').check_node('Star', missing_msg="Are you using `SELECT *` to select _all_ columns?")
)
```

***

`@instructions`
Get the name and birth date of the person born on November 11th, 1974. Remember to use ISO date format (`'1974-11-11'`)!

`@hint`
```
SELECT ___, ___
FROM ___
WHERE ___ = '___';
```

`@solution`
```{sql}
SELECT name, birthdate
FROM people
WHERE birthdate = '1974-11-11';
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').has_equal_ast()
    )
)

# Next check if right columns were included
check_all_columns().has_equal_value()
```

***

`@instructions`
Get the number of Hindi language films.

`@hint`
```
SELECT ___(___)
FROM ___
WHERE ___ = '___';
```

`@solution`
```{sql}
SELECT COUNT(*)
FROM films
WHERE language = 'Hindi';
```

`@sct`
```{python}
cnt_msg = "Are you using `COUNT(*)`?"
Ex().check_correct(
    check_column('count').has_equal_value(),
    check_node('SelectStmt').multi(
        check_node('Call', missing_msg=cnt_msg).check_edge('name', missing_msg=cnt_msg),
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').has_equal_ast()
    )
)
```

***

`@instructions`
Get all details for all films with an R certification.

`@hint`
```
SELECT ___
FROM ___
WHERE ___ = '___';
```

`@solution`
```{sql}
SELECT *
FROM films
WHERE certification = 'R';
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').has_equal_ast()
    )
)

# Now, check if all columns are correct
Ex().check_correct(
    check_all_columns().has_equal_value(),
    check_node('SelectStmt').check_node('Star', missing_msg="Are you using `SELECT *` to select _all_ columns?")
)

Ex().success_msg("Wonderful! Let's look at combining different conditions now!")
```

---

## WHERE AND

Often, you'll want to select data based on multiple conditions. You can build up your `WHERE` queries by combining multiple conditions with the `AND` keyword.

For example,

```
SELECT title
FROM films
WHERE release_year > 1994
AND release_year < 2000;
```

gives you the titles of films released between 1994 and 2000.

Note that you need to specify the column name separately for every `AND` condition, so the following would be invalid:

```
SELECT title
FROM films
WHERE release_year > 1994 AND < 2000;
```

You can add as many `AND` conditions as you need!

`@pre_exercise_code`
```{python}
connect('postgresql', 'films')
set_options(visible_tables = ['films'])
```

***

`@instructions`
Get the title and release year for all Spanish language films released before 2000.

`@hint`
```
SELECT ___, ___
FROM ___
WHERE ___ < ___
AND ___ = '___';
```

`@solution`
```{sql}
SELECT title, release_year
FROM films
WHERE release_year < 2000
AND language = 'Spanish';
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').multi(
            has_equal_ast(sql = "release_year < 2000"),
            has_equal_ast(sql = "language = 'Spanish'")
        )
    )
)

# Next check if right columns were included
Ex().check_all_columns().has_equal_value()
```

***

`@instructions`
Get all details for Spanish language films released after 2000.

`@hint`
```
SELECT ___
FROM ___
WHERE ___ > ___
AND ___ = '___';
```

`@solution`
```{sql}
SELECT *
FROM films
WHERE release_year > 2000
AND language = 'Spanish';
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').multi(
            has_equal_ast(sql = "release_year > 2000"),
            has_equal_ast(sql = "language = 'Spanish'")
        )
    )
)

# Now, check if all columns are correct
Ex().check_correct(
    check_all_columns().has_equal_value(),
    check_node('SelectStmt').check_node('Star', missing_msg="Are you using `SELECT *` to select _all_ columns?")
)
```

***

`@instructions`
Get all details for Spanish language films released after 2000, but before 2010.

`@hint`
```
SELECT ___
FROM ___
WHERE ___ > ___
AND ___ < ___
AND ___ = '___';
```

`@solution`
```{sql}
SELECT *
FROM films
WHERE release_year > 2000
AND release_year < 2010
AND language = 'Spanish';
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').multi(
            has_equal_ast(sql = "release_year > 2000"),
            has_equal_ast(sql = "release_year > 2010"),
            has_equal_ast(sql = "language = 'Spanish'")
        )
    )
)

# Now, check if all columns are correct
Ex().check_correct(
    check_all_columns().has_equal_value(),
    check_node('SelectStmt').check_node('Star', missing_msg="Are you using `SELECT *` to select _all_ columns?")
)

Ex().success_msg("Great work! Being able to combine conditions with `AND` will prove to be very useful if you only want your query to return a specific subset of records!")
```

---

## WHERE AND OR

What if you want to select rows based on multiple conditions where some but not _all_ of the conditions need to be met? For this, SQL has the `OR` operator.

For example, the following returns all films released in *either* 1994 or 2000:

```
SELECT title
FROM films
WHERE release_year = 1994
OR release_year = 2000;
```

Note that you need to specify the column for every `OR` condition, so the following is invalid:

```
SELECT title
FROM films
WHERE release_year = 1994 OR 2000;
```

When combining `AND` and `OR`, be sure to enclose the individual clauses in parentheses, like so:

```
SELECT title
FROM films
WHERE (release_year = 1994 OR release_year = 1995)
AND (certification = 'PG' OR certification = 'R');
```

Otherwise, due to SQL's precedence rules, you may not get the results you're expecting!

<hr>
What does the `OR` operator do?

`@hint`
Think about records that meet condition1 **and** condition2.

`@possible_answers`
- [Display only rows that meet at least **one** of the specified conditions.]
- Display only rows that meet **all** of the specified conditions.
- Display only rows that meet **none** of the specified conditions.

`@feedback`
- Correct!
- Incorrect. `OR` does not only display rows that meet **all** of the specified conditions.
- Incorrect. `OR` does not display rows that meet **none** of the specified conditions.

---

## WHERE AND OR (2)

You now know how to select rows that meet __some__ but not __all__ conditions by combining `AND` and `OR`.

For example, the following query selects all films that were released in 1994 or 1995 which had a rating of PG or R.

```
SELECT title
FROM films
WHERE (release_year = 1994 OR release_year = 1995)
AND (certification = 'PG' OR certification = 'R');
```

<hr>

Now you'll write a query to get the title and release year of films released in the 90s which were in French or Spanish and which took in more than $2M gross.

It looks like a lot, but you can build the query up one step at a time to get comfortable with the underlying concept in each step. Let's go!

`@pre_exercise_code`
```{python}
connect('postgresql', 'films')
set_options(visible_tables = ['films'])
```

***

`@instructions`
Get the title and release year for films released in the 90s.

`@hint`
```
SELECT ___, ___
FROM ___
WHERE ___ >= 1990 AND ___ < 2000;
```

`@solution`
```{sql}
SELECT title, release_year
FROM films
WHERE release_year >= 1990 AND release_year < 2000;
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').multi(
            has_equal_ast(sql = "release_year >= 1990"),
            has_equal_ast(sql = "release_year < 2000"),
            has_code("AND")
        )
    )
)

Ex().check_all_columns().has_equal_value()
```

***

`@instructions`
Now, build on your query to filter the records to only include French or Spanish language films.

`@hint`
```
SELECT ___, ___
FROM ___
WHERE (___ >= 1990 AND ___ < 2000)
AND (___ = 'French' OR ___ = 'Spanish');
```

`@sample_code`
```{sql}
SELECT title, release_year
FROM films
WHERE (release_year >= 1990 AND release_year < 2000)
___
```

`@solution`
```{sql}
SELECT title, release_year
FROM films
WHERE (release_year >= 1990 AND release_year < 2000)
AND (language = 'French' OR language = 'Spanish');
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').multi(
            has_equal_ast(sql = "release_year >= 1990"),
            has_equal_ast(sql = "release_year < 2000"),
            has_code("AND"),
            has_equal_ast(sql = "language = 'French'"),
            has_equal_ast(sql = "language = 'Spanish'"),
            has_code("OR")
        )
    )
)

# Next check if right columns were included
Ex().check_all_columns().has_equal_value()
```

***

`@instructions`
Finally, restrict the query to only return films that took in more than $2M gross.

`@hint`
```
SELECT ___, ___
FROM ___
WHERE (___ >= 1990 AND ___ < 2000)
AND (___ = '___' OR ___ = '___')
AND ___ > ___;
```

`@sample_code`
```{sql}
SELECT title, release_year
FROM films
WHERE (release_year >= 1990 AND release_year < 2000)
AND (language = 'French' OR language = 'Spanish')
___
```

`@solution`
```{sql}
SELECT title, release_year
FROM films
WHERE (release_year >= 1990 AND release_year < 2000)
AND (language = 'French' OR language = 'Spanish')
AND gross > 2000000;
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').multi(
            has_equal_ast(sql = "release_year >= 1990"),
            has_equal_ast(sql = "release_year < 2000"),
            has_code("AND"),
            has_equal_ast(sql = "language = 'French'"),
            has_equal_ast(sql = "language = 'Spanish'"),
            has_code("OR"),
            has_equal_ast(sql = 'gross > 2000000')
        )
    )
)

# Next check if right columns were included
check_all_columns().has_equal_value()

Ex().success_msg("That was pretty involved!")
```

---

## BETWEEN

As you've learned, you can use the following query to get titles of all films released in and between 1994 and 2000:

```
SELECT title
FROM films
WHERE release_year >= 1994
AND release_year <= 2000;
```

Checking for ranges like this is very common, so in SQL the `BETWEEN` keyword provides a useful shorthand for filtering values within a specified range. This query is equivalent to the one above:

```
SELECT title
FROM films
WHERE release_year
BETWEEN 1994 AND 2000;
```

It's important to remember that `BETWEEN` is _inclusive_, meaning the beginning and end values are included in the results!

<hr>
What does the `BETWEEN` keyword do?

`@hint`
Think about looking for values **between** a beginning and end point.

`@possible_answers`
- Filter numeric values
- Filter text values
- Filter values in a specified list
- [Filter values in a specified range]

`@feedback`
- Incorrect. `BETWEEN` does not just filter numeric values.
- Incorrect. `BETWEEN` does not just filter text values.
- Incorrect!
- Correct!

---

## BETWEEN (2)

Similar to the `WHERE` clause, the `BETWEEN` clause can be used with multiple `AND` and `OR` operators, so you can build up your queries and make them even more powerful!

For example, suppose we have a table called `kids`. We can get the names of all kids between the ages of 2 and 12 from the United States:

```
SELECT name
FROM kids
WHERE age BETWEEN 2 AND 12
AND nationality = 'USA';
```

Take a go at using `BETWEEN` with `AND` on the films data to get the title and release year of all Spanish language films released between 1990 and 2000 (inclusive) with budgets over $100 million. We have broken the problem into smaller steps so that you can build the query as you go along!

`@pre_exercise_code`
```{python}
connect('postgresql', 'films')
set_options(visible_tables = ['films'])
```

***

`@instructions`
Get the title and release year of all films released between 1990 and 2000 (inclusive).

`@hint`
```
SELECT ___, ___
FROM ___
WHERE ___ BETWEEN ___ AND ___;
```

`@solution`
```{sql}
SELECT title, release_year
FROM films
WHERE release_year BETWEEN 1990 AND 2000;
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
      	check_edge('target_list', 0).has_equal_ast(),
      	check_edge('target_list', 1).has_equal_ast(),
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').multi(
            check_edge('left').has_equal_ast(incorrect_msg = "Check the`WHERE` statement. Did you get the films released `BETWEEN` 1990 and 2000? No other conditions are necessary at this step."),
          	check_edge('op').has_equal_ast(),
            check_edge('right', 0).has_equal_ast(),
            check_edge('right', 1).has_equal_ast()
        )
    )
)

# Next check if right columns were included
Ex().check_all_columns().has_equal_value()
```

***

`@instructions`
Now, build on your previous query to select only films that have budgets over $100 million.

`@hint`
```
SELECT ___, ___
FROM ___
WHERE ___ BETWEEN ___ AND ___
AND ___ > ___;
```

`@sample_code`
```{sql}
SELECT title, release_year
FROM films
WHERE release_year BETWEEN 1990 AND 2000
___
```

`@solution`
```{sql}
SELECT title, release_year
FROM films
WHERE release_year BETWEEN 1990 AND 2000
AND budget > 100000000;
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
      	check_edge('target_list', 0).has_equal_ast(),
      	check_edge('target_list', 1).has_equal_ast(),
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').multi(
        	check_edge('left').multi(
            	check_edge('left').has_equal_ast(),
                check_edge('op').has_equal_ast(),
                check_edge('right', 0).multi(
                	check_edge('left').has_equal_ast(),
                    check_edge('op').has_equal_ast(),
                    check_edge('right').has_equal_ast()
                ),
              	check_edge('right', 1).has_equal_ast(incorrect_msg = 'Check the column in `WHERE` statement. Are you correctly selecting the films with `budget` over $100 mil?')
            ),
          	check_edge('op').has_equal_ast(),
          	check_edge('right').has_equal_ast(incorrect_msg="Did you specify the minimal budget to be `100000000` (with _8_ zeros)?")
        )
    )
)


# Next check if right columns were included
Ex().check_all_columns().has_equal_value()
```

***

`@instructions`
Now restrict the query to only return Spanish language films.

`@hint`
```
SELECT ___, ___
FROM ___
WHERE ___ BETWEEN ___ AND ___
AND ___ > ___
AND ___ = '___';
```

`@sample_code`
```{sql}
SELECT title, release_year
FROM films
WHERE release_year BETWEEN 1990 AND 2000
AND budget > 100000000
___
```

`@solution`
```{sql}
SELECT title, release_year
FROM films
WHERE release_year BETWEEN 1990 AND 2000
AND budget > 100000000
AND language = 'Spanish';
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
      	check_edge('target_list', 0).has_equal_ast(),
      	check_edge('target_list', 1).has_equal_ast(),
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').multi(
        	check_edge('left').multi(
            	check_edge('left').has_equal_ast(),
                check_edge('op').has_equal_ast(),
                check_edge('right', 0).multi(
                	check_edge('left').multi(
                      	check_edge('left',missing_msg = "Check your `WHERE` statement. It should contain 3 conditions: on release year, budget, and language. ").has_equal_ast(),
                        check_edge('op').has_equal_ast(),
                        check_edge('right').has_equal_ast()
                    ),
                    check_edge('op').has_equal_ast(),
                    check_edge('right').multi(
                    	check_edge('left').has_equal_ast(),
                        check_edge('op').has_equal_ast(),
                        check_edge('right').has_equal_ast()
                    )
                ),
              	check_edge('right', 1).has_equal_ast()
            ),
          	check_edge('op').has_equal_ast(),
          	check_edge('right').has_equal_ast()
        )
    )
)

# Next check if right columns were included
Ex().check_all_columns().has_equal_value()
```

***

`@instructions`
Finally, modify to your previous query to include all Spanish language *or* French language films with the same criteria as before. Don't forget your parentheses!

`@hint`
```
SELECT ___, ___
FROM ___
WHERE ___ BETWEEN ___ AND ___
AND ___ > ___
AND (___ = '___' OR ___ = '___');
```

`@sample_code`
```{sql}
SELECT title, release_year
FROM films
WHERE release_year BETWEEN 1990 AND 2000
AND budget > 100000000
AND language = 'Spanish';
```

`@solution`
```{sql}
SELECT title, release_year
FROM films
WHERE release_year BETWEEN 1990 AND 2000
AND budget > 100000000
AND (language = 'Spanish' OR language = 'French');
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
  has_nrows(),
  check_node('SelectStmt').multi(
    check_edge('target_list', 0).has_equal_ast(),
    check_edge('target_list', 1).has_equal_ast(),
    check_edge('from_clause').has_equal_ast(),
    check_edge('where_clause').multi(
      check_edge('right', 1, missing_msg="Check the conditions of the `WHERE` statement. Did you remember to use parentheses?").check_node("BinaryExpr", missing_msg="Check the conditions of the `WHERE` statement. Did you remember to use parentheses?").multi(
        check_edge('left').multi(
          check_edge('left').has_equal_ast(),
          check_edge('op').has_equal_ast(),
          check_edge('right', 0).has_equal_ast()
        ),
        check_edge('op').has_equal_ast(),
        check_edge('right').multi(
          check_edge('left').has_equal_ast(),
          check_edge('op').has_equal_ast(),
          check_edge('right', 0).has_equal_ast()
        )
      ),      
      check_edge('left').has_equal_ast(),
      check_edge('op').has_equal_ast(),
      check_edge('right', 0).multi(
        check_edge('left').multi(
          check_edge('left').has_equal_ast(),
          check_edge('op').has_equal_ast(),
          check_edge('right').has_equal_ast()
        ),
        check_edge('op').has_equal_ast(),
        check_edge('right').multi(
          check_edge('left').has_equal_ast(),
          check_edge('op').has_equal_ast(),
          check_edge('right').has_equal_ast()
        )
      )

    )
  )
)

# Next check if right columns were included
Ex().check_all_columns().has_equal_value()

Ex().success_msg("Well done! Off to the next filtering operator!")
```

---

## WHERE IN

As you've seen, `WHERE` is very useful for filtering results. However, if you want to filter based on many conditions, `WHERE` can get unwieldy. For example:

```
SELECT name
FROM kids
WHERE age = 2
OR age = 4
OR age = 6
OR age = 8
OR age = 10;
```

Enter the `IN` operator! The `IN` operator allows you to specify multiple values in a `WHERE` clause, making it easier and quicker to specify multiple `OR` conditions! Neat, right?

So, the above example would become simply:

```
SELECT name
FROM kids
WHERE age IN (2, 4, 6, 8, 10);
```

Try using the `IN` operator yourself!

`@pre_exercise_code`
```{python}
connect('postgresql', 'films')
set_options(visible_tables = ['films'])
```

***

`@instructions`
Get the title and release year of all films released in 1990 or 2000 that were longer than two hours. Remember, duration is in minutes!

`@hint`
```
SELECT ___, ___
FROM ___
WHERE release_year IN (___, ___)
AND ___ > ___;
```

`@solution`
```{sql}
SELECT title, release_year
FROM films
WHERE release_year IN (1990, 2000)
AND duration > 120;
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
   multi(
    has_nrows(),
    check_all_columns().has_equal_value()
   ),  
    check_edge("body", 0).multi(
      multi(
          check_edge("target_list", 0).check_edge("fields", 0).has_equal_ast(),
          check_edge("target_list", 1).check_edge("fields", 0).has_equal_ast(),
      ),
      check_edge("from_clause", 0).check_edge("fields", 0).has_equal_ast(),
      check_edge("where_clause").multi(
          check_edge("left").multi(
              check_edge("left", missing_msg="Check your `WHERE` statement. In should have 2 conditions: on the release year and on the film's duration.").check_edge("fields", 0).has_equal_ast(),
              check_edge("op").has_equal_ast(incorrect_msg="Did you use the `IN` operator to find films released in 1990 or 2000?"),
              multi(
                  check_edge("right", 0).has_equal_ast(),
                  check_edge("right", 1).has_equal_ast(),
              ),
          ),
          check_edge("op").has_equal_ast(),
          check_edge("right").multi(
              check_edge("left").check_edge("fields", 0).has_equal_ast(),
              check_edge("op").has_equal_ast(),
              check_edge("right").has_equal_ast(),
          ),
      ),
  )
)

```

***

`@instructions`
Get the title and language of all films which were in English, Spanish, or French.

`@hint`
```
SELECT ___, ___
FROM ___
WHERE ___ IN ('___', '___', '___');
```

`@solution`
```{sql}
SELECT title, language
FROM films
WHERE language IN ('English', 'Spanish', 'French');
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').has_equal_ast()
    )
)

# Next check if right columns were included
Ex().check_all_columns().has_equal_value()
```

***

`@instructions`
Get the title and certification of all films with an NC-17 or R certification.

`@hint`
```
SELECT ___, ___
FROM ___
WHERE ___ IN ('NC-17', '___');
```

`@solution`
```{sql}
SELECT title, certification
FROM films
WHERE certification IN ('NC-17', 'R');
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').has_equal_ast()
    )
)

# Next check if right columns were included
Ex().check_all_columns().has_equal_value()

Ex().success_msg("Your SQL vocabulary is growing by the minute!")
```

---

## Introduction to NULL and IS NULL

In SQL, `NULL` represents a missing or unknown value. You can check for `NULL` values using the expression `IS NULL`. For example, to count the number of missing birth dates in the `people` table:

```
SELECT COUNT(*)
FROM people
WHERE birthdate IS NULL;
```

As you can see, `IS NULL` is useful when combined with `WHERE` to figure out what data you're missing.

Sometimes, you'll want to filter out missing values so you only get results which are not `NULL`. To do this, you can use the `IS NOT NULL` operator.

For example, this query gives the names of all people whose birth dates are *not* missing in the `people` table.

```
SELECT name
FROM people
WHERE birthdate IS NOT NULL;
```

<hr>
What does `NULL` represent?

`@hint`
Remember, `NULL` represents values which are missing or unknown.

`@possible_answers`
- A corrupt entry
- [A missing value]
- An empty string
- An invalid value

`@feedback`
- Incorrect. We can not be sure that a `NULL` value is actually corrupt.
- Correct! `NULL` is used to represent unknown values.
- Incorrect. An empty string is not the same as a `NULL` value.
- Incorrect!

---

## NULL and IS NULL

Now that you know what `NULL` is and what it's used for, it's time for some practice!

`@pre_exercise_code`
```{python}
connect('postgresql', 'films')
set_options(visible_tables = ['films', 'people'])
```

***

`@instructions`
Get the names of people who are still alive, i.e. whose death date is missing.

`@hint`
```
SELECT ___
FROM ___
WHERE ___ IS NULL;
```

`@solution`
```{sql}
SELECT name
FROM people
WHERE deathdate IS NULL;
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').has_equal_ast()
    )
)

# Next check if right columns were included
Ex().check_all_columns().has_equal_value()
```

***

`@instructions`
Get the title of every film which doesn't have a budget associated with it.

`@hint`
```
SELECT ___
FROM ___
WHERE ___ ___ ___;
```

`@solution`
```{sql}
SELECT title
FROM films
WHERE budget IS NULL;
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').has_equal_ast()
    )
)

# Next check if right columns were included
Ex().check_all_columns().has_equal_value()
```

***

`@instructions`
Get the number of films which don't have a language associated with them.

`@hint`
```
SELECT ___(___)
FROM ___
WHERE language ___ ___;
```

`@solution`
```{sql}
SELECT COUNT(*)
FROM films
WHERE language IS NULL;
```

`@sct`
```{python}
cnt_msg = "Are you using `COUNT(*)`?"
Ex().check_correct(
    check_column('count').has_equal_value(),
    check_node('SelectStmt').multi(
        check_node('Call', missing_msg=cnt_msg).check_edge('name', missing_msg=cnt_msg),
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').has_equal_ast()
    )
)

Ex().success_msg("Alright! Are you ready for a last type of operator?")
```

---

## LIKE and NOT LIKE

As you've seen, the `WHERE` clause can be used to filter text data. However, so far you've only been able to filter by specifying the exact text you're interested in. In the real world, often you'll want to search for a *pattern* rather than a specific text string.

In SQL, the `LIKE` operator can be used in a `WHERE` clause to search for a pattern in a column. To accomplish this, you use something called a _wildcard_ as a placeholder for some other values. There are two wildcards you can use with `LIKE`:

The `%` wildcard will match zero, one, or many characters in text. For example, the following query matches companies like `'Data'`, `'DataC'` `'DataCamp'`, `'DataMind'`, and so on:

```
SELECT name
FROM companies
WHERE name LIKE 'Data%';
```

The `_` wildcard will match a _single_ character. For example, the following query matches companies like `'DataCamp'`, `'DataComp'`, and so on:

```
SELECT name
FROM companies
WHERE name LIKE 'DataC_mp';
```

You can also use the `NOT LIKE` operator to find records that *don't* match the pattern you specify.

Got it? Let's practice!

`@pre_exercise_code`
```{python}
connect('postgresql', 'films')
set_options(visible_tables = ['people'])
```

***

`@instructions`
Get the names of all people whose names begin with 'B'. The pattern you need is `'B%'`.

`@hint`
```
SELECT ___
FROM ___
WHERE ___ LIKE '___';
```

`@solution`
```{sql}
SELECT name
FROM people
WHERE name LIKE 'B%';
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').has_equal_ast()
    )
)

# Next check if right columns were included
check_all_columns().has_equal_value()
```

***

`@instructions`
Get the names of people whose names have 'r' as the second letter. The pattern you need is `'_r%'`.

`@hint`
```
SELECT ___
FROM ___
WHERE ___ ___ '___';
```

`@solution`
```{sql}
SELECT name
FROM people
WHERE name LIKE '_r%';
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').has_equal_ast()
    )
)

# Next check if right columns were included
Ex().check_all_columns().has_equal_value()
```

***

`@instructions`
Get the names of people whose names don't start with A. The pattern you need is `'A%'`.

`@hint`
```
SELECT ___
FROM ___
WHERE ___ NOT LIKE '___';
```

`@solution`
```{sql}
SELECT name
FROM people
WHERE name NOT LIKE 'A%';
```

`@sct`
```{python}
# First check if the WHERE clause was correct
Ex().check_correct(
    has_nrows(),
    check_node('SelectStmt').multi(
        check_edge('from_clause').has_equal_ast(),
        check_edge('where_clause').has_equal_ast()
    )
)

# Next check if right columns were included
Ex().check_all_columns().has_equal_value()

Ex().success_msg("This concludes the second chapter of the intro to SQL course. Rush over to chapter 3 if you want to learn more about aggregate functions!")
```
