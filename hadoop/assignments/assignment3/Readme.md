# Assignment 3
## Requirements
#### 题目一（简单）
展示电影 ID 为 2116 这部电影各年龄段的平均影评分。
#### 题目二（中等）
找出男性评分最高且评分次数超过 50 次的 10 部电影，展示电影名，平均影评分和评分次数
#### 题目三（选做）
找出影评次数最多的女士所给出最高分的 10 部电影的平均影评分，展示电影名和平均影评分（可使用多行 SQL）。

## Implementations
### 1. Create Database
    hive> create database yen;
    OK
    Time taken: 0.04 seconds

### 2.Use the created database
    hive> use yen;
    OK
    Time taken: 0.022 seconds

### 3. Create 3 tables
    hive> Create external table yen.t_movie(movieid INT,moviename string,movietype STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
        > WITH SERDEPROPERTIES ("field.delim"="::") LOCATION '/user/yenyen/movies/';
    OK
    Time taken: 0.15 seconds

    hive> Create external table yen.t_user(userid INT,sex string,age INT,occupation INT,zipcode STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
        > WITH SERDEPROPERTIES ("field.delim"="::")  LOCATION '/user/yenyen/users/';
    OK
    Time taken: 0.147 seconds

    hive> Create external table yen.t_rating(userid INT,movieid int,rate int, times int) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
    > WITH SERDEPROPERTIES ("field.delim"="::") LOCATION '/user/yenyen/ratings/';
    OK
    Time taken: 0.154 seconds

### 4. 展示电影 ID 为 2116 这部电影各年龄段的平均影评分。
    hive> Select age, avg(rate) from yen.t_rating a join yen.t_user b on a.userid=b.userid where a.movieid=2116 group by b.age;
    Query ID = student5_20220328185753_0f74696f-4ad1-4155-9d8f-48421af62d5f
    Total jobs = 1
    Launching Job 1 out of 1
    Status: Running (Executing on YARN cluster with App id application_1645699879292_0768)

    ----------------------------------------------------------------------------------------------
            VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED  
    ----------------------------------------------------------------------------------------------
    Map 3 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0  
    ----------------------------------------------------------------------------------------------
    VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 8.48 s     
    ----------------------------------------------------------------------------------------------
    OK
    1	3.2941176470588234
    18	3.3580246913580245
    25	3.436548223350254
    35	3.2278481012658227
    45	2.8275862068965516
    50	3.32
    56	3.5
    Time taken: 10.131 seconds, Fetched: 7 row(s)


### 5. 找出男性评分最高且评分次数超过 50 次的 10 部电影，展示电影名，平均影评分和评分次数。

    hive> Select b.sex,c.moviename, avg(rate) avgrate, count(a.movieid) as total from yen.t_rating a join yen.t_user b on a.userid=b.userid join yen.t_movie c on a.movieid=c.movieid where b.sex='M' group by b.sex,c.moviename having total>50 order by avgrate desc limit 10;
    Query ID = student5_20220328112647_2962324a-dd53-4337-9a3f-417f7484ca84
    Total jobs = 1
    Launching Job 1 out of 1
    Status: Running (Executing on YARN cluster with App id application_1645699879292_0738)

    ----------------------------------------------------------------------------------------------
            VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED  
    ----------------------------------------------------------------------------------------------
    Map 4 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Map 5 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Reducer 2 ...... container     SUCCEEDED      4          4        0        0       0       0  
    Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0  
    ----------------------------------------------------------------------------------------------
    VERTICES: 05/05  [==========================>>] 100%  ELAPSED TIME: 11.52 s    
    ----------------------------------------------------------------------------------------------
    OK
    b.sex	c.moviename	avgrate	total
    M	Sanjuro (1962)	4.639344262295082	61
    M	Godfather, The (1972)	4.583333333333333	1740
    M	Seven Samurai (The Magnificent Seven) (Shichinin no samurai) (1954)	4.576628352490421	522
    M	Shawshank Redemption, The (1994)	4.560625	1600
    M	Raiders of the Lost Ark (1981)	4.520597322348094	1942
    M	Usual Suspects, The (1995)	4.518248175182482	1370
    M	Star Wars: Episode IV - A New Hope (1977)	4.495307167235495	2344
    M	Schindler's List (1993)	4.49141503848431	1689
    M	Paths of Glory (1957)	4.485148514851486	202
    M	Wrong Trousers, The (1993)	4.478260869565218	644
    Time taken: 14.083 seconds, Fetched: 10 row(s)

### 6. 找出影评次数最多的女士所给出最高分的 10 部电影的平均影评分，展示电影名和平均影评分（可使用多行 SQL）。
#### 6a. Looks for the userid of the highest number of rating lady
    hive> Select b1.userid from yen.t_rating a1 join yen.t_user b1 on a1.userid=b1.userid where b1.sex='F' group by b1.userid order by count(a1.movieid) desc limit 1;
    Query ID = student5_20220328193215_a4abbbca-e5b2-4224-bb9b-8c65f52c6dfc
    Total jobs = 1
    Launching Job 1 out of 1
    Status: Running (Executing on YARN cluster with App id application_1645699879292_0768)

    ----------------------------------------------------------------------------------------------
            VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED  
    ----------------------------------------------------------------------------------------------
    Map 4 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Reducer 2 ...... container     SUCCEEDED      4          4        0        0       0       0  
    Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0  
    ----------------------------------------------------------------------------------------------
    VERTICES: 04/04  [==========================>>] 100%  ELAPSED TIME: 9.50 s     
    ----------------------------------------------------------------------------------------------
    OK
    1150

#### The highest number of rating lady has userid 1150. Next, look for the top 10 highest rated movie by her

    hive> Select movieid from yen.t_rating where userid=1150 order by rate desc limit 10;
    Query ID = student5_20220328193556_ce875a15-0e36-45be-811a-15be9b896811
    Total jobs = 1
    Launching Job 1 out of 1
    Status: Running (Executing on YARN cluster with App id application_1645699879292_0768)

    ----------------------------------------------------------------------------------------------
            VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED  
    ----------------------------------------------------------------------------------------------
    Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Reducer 2 ...... container     SUCCEEDED      1          1        0        0       0       0  
    ----------------------------------------------------------------------------------------------
    VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 7.68 s     
    ----------------------------------------------------------------------------------------------
    OK
    1256
    1094
    905
    2064
    2997
    750
    904
    1236
    1279
    745
    Time taken: 10.088 seconds, Fetched: 10 row(s)

#### Find the average rating of these 10 movies.

    hive> Select c.moviename,avg(a.rate) avgrate from yen.t_rating a join yen.t_movie c on a.movieid=c.movieid where a.movieid in 
        > (1256,1094,905,2064,2997,750,904,1236,1279,745 )
        > group by c.moviename;
    Query ID = student5_20220328193801_ed23015c-bee7-4f96-96b0-ffa2cd11e729
    Total jobs = 1
    Launching Job 1 out of 1
    Status: Running (Executing on YARN cluster with App id application_1645699879292_0768)

    ----------------------------------------------------------------------------------------------
            VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED  
    ----------------------------------------------------------------------------------------------
    Map 3 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0  
    ----------------------------------------------------------------------------------------------
    VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 8.79 s     
    ----------------------------------------------------------------------------------------------
    OK
    Being John Malkovich (1999)	4.125390450691656
    Crying Game, The (1992)	3.7314890154597236
    Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb (1963)	4.4498902706656915
    Duck Soup (1933)	4.21043771043771
    Rear Window (1954)	4.476190476190476
    Trust (1990)	4.188888888888889
    Close Shave, A (1995)	4.52054794520548
    It Happened One Night (1934)	4.280748663101604
    Night on Earth (1991)	3.747422680412371
    Roger & Me (1989)	4.0739348370927315
    Time taken: 10.112 seconds, Fetched: 10 row(s)

#### Combine these 3 steps using one query. However, the result is differenet due to different execution plan by HQL.

    hive> Select moviename,avg(rate) from yen.t_rating b join yen.t_movie c on b.movieid=c.movieid where b.movieid in (
        > Select a.movieid from yen.t_rating a where a.userid = (Select b1.userid from yen.t_rating a1 join yen.t_user b1 on a1.userid=b1.userid where b1.sex='F' group by b1.userid order by count(a1.movieid) desc limit 1) order by rate desc limit 10
        > ) group by moviename;
    Warning: Map Join MAPJOIN[112][bigTable=?] in task 'Map 4' is a cross product
    Query ID = student5_20220330085841_3a47e859-ef34-4f06-8094-a7b62979f281
    Total jobs = 1
    Launching Job 1 out of 1
    Status: Running (Executing on YARN cluster with App id application_1645699879292_0963)

    ----------------------------------------------------------------------------------------------
            VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED  
    ----------------------------------------------------------------------------------------------
    Map 3 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Map 11 ......... container     SUCCEEDED      1          1        0        0       0       0  
    Map 6 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Reducer 7 ...... container     SUCCEEDED      4          4        0        0       0       0  
    Reducer 8 ...... container     SUCCEEDED      1          1        0        0       0       0  
    Reducer 9 ...... container     SUCCEEDED      1          1        0        0       0       0  
    Reducer 10 ..... container     SUCCEEDED      1          1        0        0       0       0  
    Map 4 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Reducer 5 ...... container     SUCCEEDED      1          1        0        0       0       0  
    Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Reducer 2 ...... container     SUCCEEDED      6          6        0        0       0       0  
    ----------------------------------------------------------------------------------------------
    VERTICES: 11/11  [==========================>>] 100%  ELAPSED TIME: 17.41 s    
    ----------------------------------------------------------------------------------------------
    OK
    Big Lebowski, The (1998)	3.7383773928896993
    Rear Window (1954)	4.476190476190476
    Star Wars: Episode IV - A New Hope (1977)	4.453694416583082
    Sound of Music, The (1965)	3.931972789115646
    Waiting for Guffman (1996)	4.147186147186147
    Badlands (1973)	4.078838174273859
    House of Yes, The (1997)	3.4742268041237114
    Fast, Cheap & Out of Control (1997)	3.8518518518518516
    Roger & Me (1989)	4.0739348370927315
    City of Lost Children, The (1995)	4.062034739454094
    Time taken: 20.262 seconds, Fetched: 10 row(s)



