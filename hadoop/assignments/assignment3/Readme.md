# Assignment 4
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
    hive> Select age, avg(rate) from yen.t_rating a join yen.t_user b on a.userid=b.userid group by b.age;
    Query ID = student5_20220328110008_810d4fa5-864b-46dd-9ccf-5060e6997d5a
    Total jobs = 1
    Launching Job 1 out of 1
    Status: Running (Executing on YARN cluster with App id application_1645699879292_0735)

    ----------------------------------------------------------------------------------------------
            VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED  
    ----------------------------------------------------------------------------------------------
    Map 3 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Reducer 2 ...... container     SUCCEEDED      4          4        0        0       0       0  
    ----------------------------------------------------------------------------------------------
    VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 8.48 s     
    ----------------------------------------------------------------------------------------------
    OK
    1	3.549520414538238
    18	3.5075734460814227
    25	3.5452350615336385
    35	3.6181615352532375
    45	3.638061530735475
    50	3.714512346530556
    56	3.766632284682826
    Time taken: 10.135 seconds, Fetched: 7 row(s)

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

    hive> Select c.moviename,avg(rate) avgrate from yen.t_rating a join yen.t_movie c on a.movieid=c.movieid where a.movieid in (
        > (Select c1.movieid from yen.t_rating c1 where c1.userid=(Select b1.userid from yen.t_rating a1 join yen.t_user b1 on a1.userid=b1.userid where b1.sex='F' group by b1.userid order by count(a1.movieid) desc limit 1) ))
        > group by c.moviename  order by avgrate desc limit 10;
    Warning: Map Join MAPJOIN[108][bigTable=?] in task 'Map 6' is a cross product
    Query ID = student5_20220328175123_877122b9-6021-4b71-b428-c1217de4c8f6
    Total jobs = 1
    Launching Job 1 out of 1
    Status: Running (Executing on YARN cluster with App id application_1645699879292_0764)

    ----------------------------------------------------------------------------------------------
            VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED  
    ----------------------------------------------------------------------------------------------
    Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Map 5 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Map 12 ......... container     SUCCEEDED      1          1        0        0       0       0  
    Map 7 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Reducer 8 ...... container     SUCCEEDED      4          4        0        0       0       0  
    Reducer 9 ...... container     SUCCEEDED      1          1        0        0       0       0  
    Reducer 10 ..... container     SUCCEEDED      1          1        0        0       0       0  
    Reducer 11 ..... container     SUCCEEDED      1          1        0        0       0       0  
    Map 6 .......... container     SUCCEEDED      1          1        0        0       0       0  
    Reducer 2 ...... container     SUCCEEDED      6          6        0        0       0       1  
    Reducer 3 ...... container     SUCCEEDED      6          6        0        0       0       1  
    Reducer 4 ...... container     SUCCEEDED      1          1        0        0       0       0  
    ----------------------------------------------------------------------------------------------
    VERTICES: 12/12  [==========================>>] 100%  ELAPSED TIME: 18.68 s    
    ----------------------------------------------------------------------------------------------
    OK
    Shawshank Redemption, The (1994)	4.554557700942973
    Godfather, The (1972)	4.524966261808367
    Close Shave, A (1995)	4.52054794520548
    Usual Suspects, The (1995)	4.517106001121705
    Schindler's List (1993)	4.510416666666667
    Wrong Trousers, The (1993)	4.507936507936508
    Sunset Blvd. (a.k.a. Sunset Boulevard) (1950)	4.491489361702127
    Raiders of the Lost Ark (1981)	4.477724741447892
    Rear Window (1954)	4.476190476190476
    Star Wars: Episode IV - A New Hope (1977)	4.453694416583082
    Time taken: 20.241 seconds, Fetched: 10 row(s)







