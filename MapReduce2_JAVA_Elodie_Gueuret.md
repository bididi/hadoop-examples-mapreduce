# MapReduce2 JAVA



Ici le lien vers mon git et mon code de JAVA,

 https://github.com/bididi/hadoop-examples-mapreduce.git

##  1.8.1 Districts containing trees

Here we have the list of the districts containing trees.

```
yarn jar /home/egueuret/hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \dt /user/egueuret/trees.csv /user/egueuret/dt

-sh-4.2$ hdfs dfs -cat /user/egueuret/dt/part-r-00000 
```

```
11
12
13
14
15
16
17
18
19
20
3
4
5
6
7
8
9
```

## 1.8.2 Show all existing species

```
yarn jar /home/egueuret/hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \species /user/egueuret/trees.csv /user/egueuret/species

-sh-4.2$ hdfs dfs -cat /user/egueuret/species/part-r-00000 
```

```
araucana
atlantica
australis
baccata
bignonioides
biloba
bungeana
cappadocicum
carpinifolia
colurna
coulteri
decurrens
dioicus
distichum
excelsior
fraxinifolia
giganteum
giraldii
glutinosa
grandiflora
hippocastanum
ilex
involucrata
japonicum
kaki
libanii
monspessulanum
nigra
nigra laricio
opalus
orientalis
papyrifera
petraea
pomifera
pseudoacacia
sempervirens
serrata
stenoptera
suber
sylvatica
tomentosa
tulipifera
ulmoides
virginiana
x acerifolia
```

## 1.8.3 Number of trees by species

```
yarn jar /home/egueuret/hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \tbs /user/egueuret/trees.csv /user/egueuret/tbs

-sh-4.2$ hdfs dfs -cat /user/egueuret/tbs/part-r-00000
```

```
araucana        1
atlantica       2
australis       1
baccata 2
bignonioides    1
biloba  5
bungeana        1
cappadocicum    1
carpinifolia    4
colurna 3
coulteri        1
decurrens       1
dioicus 1
distichum       3
excelsior       1
fraxinifolia    2
giganteum       5
giraldii        1
glutinosa       1
grandiflora     1
hippocastanum   3
ilex    1
involucrata     1
japonicum       1
kaki    2
libanii 2
monspessulanum  1
nigra   3
nigra laricio   1
opalus  1
orientalis      8
papyrifera      1
petraea 2
pomifera        1
pseudoacacia    1
sempervirens    1
serrata 1
stenoptera      1
suber   1
sylvatica       8
tomentosa       2
tulipifera      2
ulmoides        1
virginiana      2
x acerifolia    11
```

## 1.8.4 Maximum height per specie of tree (average)

```
yarn jar /home/egueuret/hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \highest /user/egueuret/trees.csv /user/egueuret/highest

-sh-4.2$ hdfs dfs -cat /user/egueuret/highest/part-r-00000
```

```
araucana        9.0
atlantica       25.0
australis       16.0
baccata 13.0
bignonioides    15.0
biloba  33.0
bungeana        10.0
cappadocicum    16.0
carpinifolia    30.0
colurna 20.0
coulteri        14.0
decurrens       20.0
dioicus 10.0
distichum       35.0
excelsior       30.0
fraxinifolia    27.0
giganteum       35.0
giraldii        35.0
glutinosa       16.0
grandiflora     12.0
hippocastanum   30.0
ilex    15.0
involucrata     12.0
japonicum       10.0
kaki    14.0
libanii 30.0
monspessulanum  12.0
nigra   30.0
nigra laricio   30.0
opalus  15.0
orientalis      34.0
papyrifera      12.0
petraea 31.0
pomifera        13.0
pseudoacacia    11.0
sempervirens    30.0
serrata 18.0
stenoptera      30.0
suber   10.0
sylvatica       30.0
tomentosa       20.0
tulipifera      35.0
ulmoides        12.0
virginiana      14.0
x acerifolia    45.0
```

## 1.8.5 Sort the trees height from smallest to largest (average)

```
yarn jar /home/egueuret/hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \sort /user/egueuret/trees.csv /user/egueuret/sort

-sh-4.2$ hdfs dfs -cat /user/egueuret/sort/part-r-00000
```

```
2.0
5.0
6.0
9.0
10.0
10.0
10.0
10.0
10.0
11.0
12.0
12.0
12.0
12.0
12.0
12.0
12.0
12.0
13.0
13.0
14.0
14.0
14.0
15.0
15.0
15.0
15.0
15.0
16.0
16.0
16.0
16.0
18.0
18.0
18.0
18.0
20.0
20.0
20.0
20.0
20.0
20.0
20.0
20.0
20.0
20.0
20.0
20.0
22.0
22.0
22.0
22.0
22.0
23.0
25.0
25.0
25.0
25.0
25.0
25.0
26.0
27.0
27.0
28.0
30.0
30.0
30.0
30.0
30.0
30.0
30.0
30.0
30.0
30.0
30.0
30.0
30.0
30.0
30.0
30.0
30.0
31.0
31.0
32.0
33.0
34.0
35.0
35.0
35.0
35.0
35.0
40.0
40.0
40.0
42.0
45.0
```

## 1.8.6 District containing the oldest tree (difficult)

The district with the oldest tree is the 5.

```
yarn jar /home/egueuret/hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \otbd /user/egueuret/trees.csv /user/egueuret/otbd

-sh-4.2$ hdfs dfs -cat /user/egueuret/otbd/part-r-00000
```

```
5
```

## 1.8.7 District containing the most trees (very difficult)

We have two job for this exercise, here is the first one: 

in the first column we have the districts and in the second column we have the number of tree in the district

```
yarn jar /home/egueuret/hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \ntbd /user/egueuret/trees.csv /user/egueuret/ntbd

-sh-4.2$ hdfs dfs -cat /user/egueuret/ntbd/Job/part-r-00000
```

```
11      1
12      29
13      2
14      3
15      1
16      36
17      1
18      1
19      6
20      3
3       1
4       1
5       2
6       1
7       3
8       5
9       1
```

here, the second one:

The district with the most of tree is the 16 with 36 trees

```
yarn jar /home/egueuret/hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \ntbd /user/egueuret/trees.csv /user/egueuret/ntbd

-sh-4.2$ hdfs dfs -cat /user/egueuret/ntbd/Job1/part-r-00000
```

```
16      36
```

