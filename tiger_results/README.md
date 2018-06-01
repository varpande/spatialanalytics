
# US Census Tiger Dataset

We used the LineString dataset from the TIGER dataset which contains approximately 70 million linestrings in US. There are other datasets in TIGER but they are limited in size (less than 2 million spatial objects). To have a larger dataset to join with, we generated a rectangle dataset by computing bounding boxes of these linestrings. We also sampled 170 million points that are in US from the OSM dataset to join with these datasets.

## Distance Join Performance

<p align="center">
  <img width="700" height="360" src="./images/distance_join_cost_breakdown.jpg">
</p>

Distance join costs breakdown

<p align="center">
  <img width="700" height="360" src="./images/distance_join_scalability.jpg">
</p>

Distance join scalability

<p align="center">
  <img width="700" height="360" src="./images/distance_join_shuffle.jpg">
</p>

Distance join shuffle costs

---


## Spatial Joins Performance

<p align="center">
  <img width="1000" height="360" src="./images/joins_scalability.jpg">
</p>

Spatial joins scalability

<p align="center">
  <img width="700" height="360" src="./images/joins_peak_memory.jpg">
</p>

Spatial joins peak execution memory consumption

<p align="center">
  <img width="700" height="360" src="./images/joins_shuffle_writes.jpg">
</p>

Spatial joins shuffle write costs


<p align="center">
  <img width="700" height="360" src="./images/joins_shuffle_reads.jpg">
</p>

Spatial joins shuffle read costs

<p align="center">
  <img width="600" height="360" src="./images/spatial_joins.jpg">
</p>

Spatial joins costs breakdown on a single node

<p align="center">
  <img width="700" height="360" src="./images/spatial_joins_breakdown.jpg">
</p>

Point-Rectangle spatial join costs breakdown scalinng up the number of nodes

---

## kNN Join Performance

<p align="center">
  <img width="700" height="360" src="./images/knn_join_cost_breakdown.jpg">
</p>

kNN join costs breakdown

<p align="center">
  <img width="700" height="360" src="./images/knn_join_scalability.jpg">
</p>

kNN join scalability

<p align="center">
  <img width="700" height="360" src="./images/knn_join_shuffle.jpg">
</p>

kNN join shuffle costs


