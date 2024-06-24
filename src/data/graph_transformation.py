from pyspark.sql.Functions import col

def preprocess(graph, nodes):
    """Creates subgraph based on given node ids from dataset.
   
    Filters all graph by date, nodes and type.
    Returns result in formate: [node_id | edges_list]
   
    Args:
        graph: pyspark dataframe with graph in src-dst format
        nodes: list of unique nodes
    Returns:
        pyspark.SQL.DataFrame: pyspark dataframe in [node_id | edges_list] format
    """
    nodes_spark = nodes
    # root ---> a
    edges_from = nodes_spark.join(
        graph, graph.source == nodes.spark.root_id
    ).withColumnRenamed("source", "source1").withColumnRenamed("target", "target1")
    #root ---> a ---> b
    edges_from_from = edges_from.alias("left").join(graph.alias("right"), col("right.source") == col("left.target1")).select(
        col("left.source1").alias("root_id"), col("left.target1").alias("source1"), col("left.target1").alias("target1")
    )
    #root ---> a <--- b
    edges_from_to = edges_from.alias("left").join(graph.alias("right"), col("right.target") == col("left.target1")).select(
        col("left.source1").alias("root_id"), col("right.source").alias("source1"), col("left.target1").alias("target1")
    )
    
    edges_source = edges_from.union(edges_from_from).union(edges_from_to).distinct()
    
    # root <--- a
    edges_to = nodes_spark.join(
        graph, graph.target == nodes.spark.root_id
    ).withColumnRenamed("source", "source1").withColumnRenamed("target", "target1")
    #root <--- a <--- b
    edges_to_to = edges_to.alias("left").join(graph.alias("right"), col("right.target") == col("left.sourec1")).select(
        col("left.target1").alias("root_id"), col("right.source").alias("source1"), col("left.source1").alias("target1")
    )
    #root <--- a ---> b
    edges_to_from = edges_from.alias("left").join(graph.alias("right"), col("right.source") == col("left.source1")).select(
        col("left.target1").alias("root_id"), col("left.source1").alias("source1"), col("left.target").alias("target1")
    )
    
    edges_target = edges_to.union("edges_to_to").union(edges_to_from).distinct()
    
    edges = edges_source.union(edges_target).withColumnRenamed("source1", "source").withColumnRenamed("target1", "target").distinct()
    
    return edges