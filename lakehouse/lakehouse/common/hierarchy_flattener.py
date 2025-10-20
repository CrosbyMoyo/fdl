# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType
from typing import List
from pyspark.sql.functions import (
    col,
    lit,
    concat,
    when,
    expr,
    split,
    array,
    struct,
    size,
    upper,
    max,
    row_number
)

# COMMAND ----------

# MAGIC %run ./properties

# COMMAND ----------

class HierarchyFlattener():
    def sap_s4hana_hierarchy_flattener(self, setheader: DataFrame, setnode: DataFrame, setleaf: DataFrame, setclst: DataFrame, setclass: str, hier_id: str) -> DataFrame:
        """
        Flattens a hierarchical structure into a wide table format.

        Parameters:
        setheader (DataFrame): DataFrame containing the SETHEADER data.
        setnode (DataFrame): DataFrame containing the SETNODE data.
        setleaf (DataFrame): DataFrame containing the SETLEAF data.
        setclst (DataFrame): DataFrame containing the SETCLST data.
        setclass (str): The set class to filter the data.
        hier_id (str): The hierarchy ID to filter the data.

        Returns:
        DataFrame: A wide flattened table with hierarchical levels as columns.

        Steps:
        1) Filter SETHEADER, SETNODE, SETLEAF based on the provided setclass.
        2) Separate SETLEAF into EQ and BT value options.
        3) Expand BT value options by joining with the corresponding description table.
        4) Unify SETHEADER, SETNODE, SETLEAF into one DataFrame.
        5) Identify root nodes and initialize the hierarchy traversal.
        6) Use BFS logic to traverse and flatten the hierarchy from root to leaves.
        7) Mark leaf nodes dynamically during traversal.
        8) Determine the maximum depth of the hierarchy.
        9) Convert the hierarchical path to an array of strings and pivot out columns for each level.
        10) Populate null LevelX_Node columns for leaf nodes.
        11) Add HIER_ID as the root of the hierarchy.
        12) Join with SETCLST to get descriptive columns for SETCLASS.
        13) Transform columns to get the final flattened structure.

        Notes:
        - The function uses BFS logic to traverse the hierarchy.
        - The resulting DataFrame contains columns for each level of the hierarchy.
        - The function dynamically marks leaf nodes and populates null columns for leaf nodes.
        """

        def get_leaf_for_union(setleaf: DataFrame, leafdesc: DataFrame, leaf_id_column: str) -> DataFrame:
            """
            Handles the "EQ" and "BT" logic for "VALOPTION" column in the "SETLEAF" table.

            Parameters:
            setleaf (DataFrame): DataFrame containing the SETLEAF data.
            leafdesc (DataFrame): DataFrame containing the relative master data for specific hieararchies.
            leaf_id_column (str): The leaf ID column name to be used in the joins.

            Returns:
            DataFrame: A unified table to handle the "BT" and "EQ" logic.
            """

            # separate the setleaf dataframe into two
            leaf_eq = setleaf.filter(col("VALOPTION") == "EQ")

            leaf_eq_for_union = (
                leaf_eq.selectExpr(
                    "SETNAME as parent_node_id",
                    "VALFROM as child_node_id",
                    "SETCLASS as hier_type_id",
                    "SUBCLASS as sub_key_id",
                    "SEQNR as leaf_seqnr"
                )
                    .dropDuplicates()
            )

            # Identify potential roots
            setleaf_setnames = leaf_eq.select("SETNAME").distinct()

            setnode_subsetnames = setnode.select("SUBSETNAME").distinct()

            # Perform a left anti-join to find potential roots
            potential_roots = (
                setleaf_setnames
                    .join(
                        setnode_subsetnames,
                            setleaf_setnames["SETNAME"] == setnode_subsetnames["SUBSETNAME"],
                        how="left_anti"
                    ).selectExpr(
                        "SETNAME as root_id"
                    )
            )

            # Add a "potential_root" flag to the original DataFrame
            leaf_eq_for_union = (
                leaf_eq_for_union
                    .join(
                        potential_roots,
                            leaf_eq_for_union["parent_node_id"] == potential_roots["root_id"],
                        how="left"
                    ).selectExpr(
                        "*",
                        """
                        CASE 
                            WHEN root_id IS NOT NULL THEN true 
                            ELSE false 
                        END AS is_root
                        """
                    ).drop("root_id")
            )  # Drop the helper column

            # arrange the dataframe to unify with setheader and setnode dataframes
            leaf_for_union = (leaf_eq_for_union)

            return leaf_for_union
        
        def get_node_for_union(setnode: DataFrame) -> DataFrame:
            node_for_union = (
                setnode
                    .selectExpr(
                        "SETNAME as parent_node_id",
                        "SUBSETNAME as child_node_id",
                        "SETCLASS as hier_type_id",
                        "SUBCLASS as sub_key_id",
                        "SEQNR as node_seqnr"
                    )
            )
            # Extract distinct values for SETNAME and SUBSETNAME
            setnode_setnames = node_for_union.select("parent_node_id").distinct()
            setnode_subsetnames = node_for_union.select("child_node_id").distinct()

            # Identify root nodes by performing a left anti-join
            roots = (
                setnode_setnames
                    .join(
                        setnode_subsetnames,
                            setnode_setnames["parent_node_id"] == setnode_subsetnames["child_node_id"],
                        how="left_anti"
                    ).selectExpr(
                        "parent_node_id as is_root_id"
                    )
            )

            # Add the `is_root` column to the original DataFrame
            node_for_union = (
                node_for_union
                    .join(
                        roots,
                            node_for_union["parent_node_id"] == roots["is_root_id"],
                        how="left"
                    ).selectExpr(
                        "*",
                        "is_root_id IS NOT NULL AS is_root"
                    ).drop("is_root_id")
            )  # Drop the helper column

            # Final selection
            node_for_union = (
                node_for_union
                    .select(
                        "parent_node_id", 
                        "child_node_id", 
                        "hier_type_id", 
                        "sub_key_id", 
                        "node_seqnr", 
                        "is_root"
                    )
            )

            return node_for_union
        
        def get_unified_set_tables(setheader: DataFrame, node_for_union: DataFrame, leaf_for_union: DataFrame) -> DataFrame:
            """
            Unifies the SETHEADER, SETNODE, and SETLEAF dataframes into one to get the entire hierarchy structure.

            Parameters:
            setheader (DataFrame): DataFrame containing the SETHEADER data.
            setnode (DataFrame): DataFrame containing the SETNODE data.
            leaf_for_union (DataFrame): DataFrame containing the SETLEAF data, processed with the "EQ" and "BT" logic.

            Returns:
            DataFrame: A unified table combining SETHEADER, SETNODE, and SETLEAF into a single hierarchy structure.
            """

            # union setheader, setnode, setleaf all together
            hierarchy_all = (
                node_for_union
                    .unionByName(
                        leaf_for_union, allowMissingColumns=True
                    )
            )

            return hierarchy_all
        
        def get_dfs_for_bfs(hierarchy_all: DataFrame) -> (DataFrame, DataFrame):
            """
            Initializes DataFrames for performing a breadth-first search (BFS) on the hierarchy structure.

            Parameters:
            hierarchy_all (DataFrame): DataFrame containing the unified hierarchy structure.

            Returns:
            (DataFrame, DataFrame): 
                - The first DataFrame (`accumulated`) stores the entire hierarchy structure as it is traversed.
                - The second DataFrame (`current_level`) tracks nodes at the current level of traversal.
            """

            # identify root(s)
            # typically, root is where is_root flag is true
            roots = (
                hierarchy_all
                    .filter(
                        (col('is_root') == True)
                    ).selectExpr(
                        "parent_node_id as child_node_id",
                        "CAST(NULL AS STRING) as parent_node_id",
                        "1 as level",
                        "CAST(parent_node_id AS STRING) as path",
                        "hier_type_id",
                        "sub_key_id",
                        "false as leaf_flag",
                        "CAST(NULL AS STRING) as leaf_seqnr",
                        "CAST(NULL AS STRING) as node_seqnr",
                        "is_root"
                    )
            )

            accumulated = roots.select(
                "child_node_id",
                "parent_node_id",
                "level",
                "path",
                "hier_type_id",
                "sub_key_id",
                "leaf_flag",
                "node_seqnr",
                "leaf_seqnr",
                "is_root"
            )

            current_level = roots.select(
                "child_node_id",
                "parent_node_id",
                "level",
                "path",
                "hier_type_id",
                "sub_key_id",
                "leaf_flag",
                "node_seqnr",
                "leaf_seqnr",
                "is_root"
            )

            return accumulated, current_level
        
        def get_setleaf_nodes(leaf_union: DataFrame) -> DataFrame:
            """
            Extracts the setleaf nodes from the unified setleaf DataFrame.

            Parameters:
            leaf_union (DataFrame): DataFrame containing the unified setleaf data.

            Returns:
            DataFrame: A DataFrame containing distinct setleaf nodes with columns for parent node, child node, and sequence number.
            """
            # identify leaf nodes
            setleaf_nodes = leaf_union.select(
                col("parent_node_id").alias("leaf_parent_node"),
                col("child_node_id").alias("leaf_child_node"),
                col("leaf_seqnr").alias("temp_leaf_seqnr")
            ).distinct()

            return setleaf_nodes
        
        def get_nodes(setnodes_union: DataFrame) -> DataFrame:
            setnodes_nodes = setnodes_union.select(
                col("parent_node_id").alias("leaf_parent_node"),
                col("child_node_id").alias("leaf_child_node"),
                col("node_seqnr").alias("temp_node_seqnr")
            ).distinct()

            return setnodes_nodes
        
        def traverse_hierarchy(hierarchy_all: DataFrame, accumulated: DataFrame, current_level: DataFrame, setleaf_nodes: DataFrame, nodes: DataFrame) -> DataFrame:
            """
            Traverses the hierarchy structure using breadth-first search (BFS) and dynamically marks leaf nodes.

            Parameters:
            hierarchy_all (DataFrame): DataFrame containing the unified hierarchy structure.
            accumulated (DataFrame): DataFrame to store the accumulated hierarchy structure during traversal.
            current_level (DataFrame): DataFrame tracking the nodes at the current level of traversal.
            setleaf_nodes (DataFrame): DataFrame containing information about leaf nodes.

            Returns:
            DataFrame: The accumulated DataFrame containing the entire hierarchy with updated leaf flags and sequence numbers.
            """

            level = 1
            while True:
                level += 1
                children = (
                    current_level.alias("p")
                    .join(
                        hierarchy_all.alias("c"),
                        on=[
                            # the parent's "child_node_id" becomes the child's "parent_node_id"
                            col("p.child_node_id") == col("c.parent_node_id")
                        ],
                        how="inner",
                    )
                    .selectExpr(
                        "c.child_node_id as child_node_id",
                        "c.parent_node_id as parent_node_id",
                        "(p.level + 1) as level",
                        "concat(p.path, '>', c.child_node_id) as path",
                        "c.hier_type_id as hier_type_id",
                        "c.sub_key_id as sub_key_id",
                        "p.leaf_flag as leaf_flag",
                        "p.leaf_seqnr as leaf_seqnr",
                        "p.node_seqnr as node_seqnr",
                        "c.is_root as is_root"
                    )
                )

                if children.isEmpty():
                    break
                
                # handling an edge case, where a profit center is also a profit center group
                if level > 2:
                    children = children.filter(col("is_root") == False)        

                # mark leaf nodes dynamically by joining with setleaf_nodes and add the leaf seqnr
                children = (
                    children
                        .join(
                            setleaf_nodes.alias("leaf_nodes"),
                            on = [
                                (children["child_node_id"] == setleaf_nodes["leaf_child_node"]) &
                                (children["parent_node_id"] == setleaf_nodes["leaf_parent_node"])
                            ],
                            how = "left_outer"
                        ).selectExpr(
                            "child_node_id",
                            "parent_node_id",
                            "hier_type_id",
                            "sub_key_id",
                            "is_root",
                            "node_seqnr",
                            "level",
                            "path",
                            """
                            CASE 
                                WHEN leaf_nodes.temp_leaf_seqnr IS NOT NULL THEN true 
                                ELSE leaf_flag 
                            END AS leaf_flag
                            """,
                            """
                            CASE 
                                WHEN leaf_nodes.temp_leaf_seqnr IS NOT NULL THEN leaf_nodes.temp_leaf_seqnr 
                                ELSE leaf_seqnr 
                            END AS leaf_seqnr
                            """
                        ).drop(
                            "temp_leaf_seqnr",
                            "leaf_child_node",
                            "leaf_parent_node"
                        )
                )
                
                # add the node seqnr dynamically
                children = (
                    children
                        .join(
                            nodes.alias("nodes"),
                            on = [
                                (children["child_node_id"] == nodes["leaf_child_node"]) &
                                (children["parent_node_id"] == nodes["leaf_parent_node"])
                            ],
                            how = "left_outer"
                        ).selectExpr(
                            "child_node_id",
                            "parent_node_id",
                            "hier_type_id",
                            "sub_key_id",
                            "is_root",
                            "level",
                            "path",
                            "leaf_flag",
                            "leaf_seqnr",
                            """
                            CASE 
                                WHEN nodes.temp_node_seqnr IS NOT NULL THEN nodes.temp_node_seqnr 
                                ELSE node_seqnr 
                            END AS node_seqnr
                        """
                    ).drop(
                        "temp_node_seqnr", 
                        "leaf_child_node", 
                        "leaf_parent_node"
                    )
            )
                
                accumulated = accumulated.unionByName(children).distinct()
                current_level = children        

            return accumulated
        
        def get_nodes_and_descriptions(setclass: str, wide: DataFrame, setheader: DataFrame, setnode: DataFrame, leafdesc: DataFrame, max_depth: int, leaf_id_column: str, leaf_desc_column: str) -> DataFrame:
            """
            Adds hierarchical node and description columns to the input DataFrame for a specified depth and enriches leaf nodes with descriptions.

            Parameters:
            setclass (str): Set class to process.
            wide (DataFrame): DataFrame containing the hierarchical structure with paths.
            setheader (DataFrame): DataFrame containing header-level descriptions for nodes.
            leafdesc (DataFrame): DataFrame containing leaf-level descriptions.
            max_depth (int): Maximum depth of the hierarchy to process.
            leaf_id_column (str): Column name for the leaf ID in the `leafdesc` DataFrame.
            leaf_desc_column (str): Column name for the leaf description in the `leafdesc` DataFrame.

            Returns:
            DataFrame: DataFrame with added hierarchical level node and description columns, and enriched leaf descriptions.
            """

            setnode = setnode.select(
                col("SETNAME").alias("sn"),
                col("SUBSETNAME").alias("cni"),
                col("SEQNR").alias("temp_node_seqnr")
            )

            for i in range(max_depth):
                level_num = i + 1
                col_name = f"level{level_num}_node"

                wide = wide.withColumn(
                    col_name,
                    col("path_array")[i]
                )  

            for i in range(max_depth):
                level_num = i + 1
                col_name = f"level{level_num}_node"
                if level_num < max_depth:
                    col_child_name = f"level{level_num + 1}_node"
                    col_seqnr = f"level{level_num + 1}_seqnr"
                col_desc = f"level{level_num}_text"


                # logic to fill the LEVELX_TEXT columns
                # partition by SETNAME
                window_spec_setheader = Window.partitionBy("SETNAME").orderBy(
                    when(col("LANGU") == "E", 1)  # prioritize LANGU = 'E'
                    .when(col("LANGU") == "F", 2)  # then prioritize LANGU = 'F'
                    .when(col("LANGU") == "P", 3)  # finally prioritize LANGU = 'P'
                    .otherwise(4),                 # fallback for other values
                    col("LANGU").desc()            # fallback to other LANGU values in descending order if all else is equal
                )

                # deduplicate the SETNAME and LANGU values from the setheader table
                setheader_deduped = setheader.withColumn(
                    "row_number",
                    row_number().over(window_spec_setheader)
                ).filter(col("row_number") == 1).drop("row_number")  # Keep only the first row   
                
                # join the setheader table with the wide table to get description for each of the description columns
                wide = wide.join(
                    setheader_deduped.select("SETNAME", "DESCRIPT"),
                    on = [
                        wide[col_name] == setheader_deduped["SETNAME"]
                    ],
                    how="left_outer"
                )

                wide = wide.alias("w").join(
                    setnode.alias("n"),
                    on = [
                        (col(f"w.{col_name}") == col("n.sn")) &
                        (col(f"w.{col_child_name}") == col("n.cni"))
                    ],
                    how="left_outer"
                )

                wide = wide.withColumn( # get the column description from the setheader table
                    col_desc,
                    col("DESCRIPT")
                ).withColumn(
                    col_seqnr,
                    col("n.temp_node_seqnr")
                ).drop("SETNAME", "DESCRIPT")
            
            if (setclass == "0101" or setclass == "0106"):
                # join the leafdesc table to get the description of the leaves

                window_spec_leafdesc = Window.partitionBy(leaf_id_column, "KOKRS").orderBy(
                    when(col("SPRAS") == "E", 1)  # prioritize SPRAS = 'E'
                    .when(col("SPRAS") == "F", 2)  # then prioritize SPRAS = 'F'
                    .when(col("SPRAS") == "P", 3)  # then prioritize SPRAS = 'P'
                    .otherwise(4),                 # fallback for other SPRAS values
                    col("SPRAS").desc()            # fallback to other SPRAS values in descending order if tied
                )

                leafdesc_deduped = leafdesc.withColumn(
                    "row_number",
                    row_number().over(window_spec_leafdesc)
                ).filter(col("row_number") == 1).drop("row_number")

                wide = wide.join(
                    leafdesc_deduped.select(leaf_id_column, leaf_desc_column, "KOKRS", "SPRAS", "DATBI"),
                    on=(
                        (wide["leaf_flag"] == True) &
                        (wide["child_node_id"] == leafdesc[leaf_id_column]) &
                        (wide["sub_key_id"] == leafdesc["KOKRS"]) &
                        (leafdesc_deduped["DATBI"] == lit("99991231"))
                    ),
                    how="left_outer"
                )
            else:
                wide = wide.join(
                    leafdesc.select(leaf_id_column, leaf_desc_column, "SPRAS"), # this join needs to change for G/L account and Cost Element hierarchies
                    on=(
                        (wide["leaf_flag"] == True) &
                        (wide["child_node_id"] == leafdesc[leaf_id_column]) &
                        (leafdesc["SPRAS"] == "E")
                    ),
                    how="left_outer"
                )

            return wide
        
        def fill_null_values(wide: DataFrame, max_depth: int, leaf_desc_column: str) -> DataFrame:
            """
            Fills null values in hierarchical node and description columns based on the last defined level and leaf descriptions.

            Parameters:
            wide (DataFrame): DataFrame containing hierarchical levels and leaf descriptions.
            max_depth (int): Maximum depth of the hierarchy to process.
            leaf_desc_column (str): Column name for the leaf description in the `wide` DataFrame.

            Returns:
            DataFrame: DataFrame with null values in level node and description columns filled.
            """

            for i in range(max_depth):
                col_name = f"level{i + 1}_node"
                col_desc = f"level{i + 1}_text"
                if i + 1 < max_depth:
                    col_child_name = f"level{i + 2}_seqnr"

                wide = (
                    wide
                        .withColumn(
                            col_name,
                            when(
                                (col("leaf_flag") == True) & (i + 1 > col("last_defined_level")),  # choose after the last defined level
                                expr("path_array[size(path_array) - 1]")  # use the last element of the path_array to populate the rest of the LevelX_Node columns
                            ).otherwise(
                                col(col_name)  # keep the original value
                            )
                        )
                )

                # fill the description column for leaves
                wide = (
                    wide
                        .withColumn(
                            col_desc,
                            when(
                                (col("leaf_flag") == True) & (i + 1 >= col("last_defined_level")),
                                wide[leaf_desc_column]
                            ).otherwise(
                                col(col_desc)
                            )
                        ).withColumn(
                            col_child_name,
                            when(
                                (col("leaf_flag") == True) & (i + 2 >= col("last_defined_level")),
                                wide["leaf_seqnr"]
                            ).otherwise(
                                col(col_child_name)
                            )
                        )
                )

            wide = wide.drop("last_defined_level", "temp_node_seqnr", "sn", "cni")

            return wide
    
        setheader = setheader.filter((col("SETCLASS") == setclass))
        setnode = setnode.filter((col("SETCLASS") == setclass))
        setleaf = setleaf.filter((col("SETCLASS") == setclass))
        

        # define the setclass and leaf description tables directories with their ID columns and description columns
        leaf_config = {
            "0106": (f"{env_vars.bronze_catalog}.fivetran_s4p.cepct", "PRCTR", "LTEXT"),
            "0101": (f"{env_vars.bronze_catalog}.fivetran_s4p.cskt", "KOSTL", "LTEXT"),
            "0102": (f"{env_vars.bronze_catalog}.fivetran_s4p.csku", "KSTAR", "LTEXT"),
            "0109": (f"{env_vars.bronze_catalog}.fivetran_s4p.skat", "SAKNR", "TXT50")
        }

        # get the related leaf description table directory, ID column, and description column
        leaf_desc_table_dir, leaf_id_column, leaf_desc_column = leaf_config[setclass]

        # get the leaf description table    
        leafdesc = spark.table(leaf_desc_table_dir)

        setnodes_union = get_node_for_union(setnode)
        
        # union setleaf to create the combined hierachy dataframe
        leaf_union = get_leaf_for_union(setleaf, leafdesc, leaf_id_column)
            
        # unify the set tables to get all of the hierarchies in one dataframe
        hierarchy_all = get_unified_set_tables(setheader, setnodes_union, leaf_union)
            
        # to be used in the traversal, create an accumulated dataframe to store the entire hierarchy structure, 
        # and the current_level dataframe to track nodes at the current level of traversal
        accumulated, current_level = get_dfs_for_bfs(hierarchy_all)
            
        # get the setleaf nodes to be used for the traversal
        setleaf_nodes = get_setleaf_nodes(leaf_union)
            
        nodes = get_nodes(setnodes_union)
            
        # traverse the hierarchy and rewrite the accumulated dataframe
        traversed = traverse_hierarchy(hierarchy_all, accumulated, current_level, setleaf_nodes, nodes)
        
        # convert"path" => array of strings, then pivot out columns
        with_array = traversed.withColumn("path_array", split(col("path"), ">"))

        # max depth logic depending on hier_id
        if hier_id is None:
            max_level = with_array.agg(max("level").alias("max_level"))
            max_depth = max_level.collect()[0]["max_level"]
        else: 
            with_array = with_array.filter(col("path_array")[0] == hier_id)
            max_level = with_array.agg(max("level").alias("max_level"))
            max_depth = max_level.collect()[0]["max_level"]

        # define the depth for each row
        with_ldf = with_array.withColumn(
            "last_defined_level",
            concat((size(col("path_array")))) # each of the row will have last_defined_level which will be the size of the path_array
        )   # this will be used to populate LevelX_Nodes when the leaf_flag is true

        # create LevelX_Node columns depending on the depth of the hierarchy
        with_nodes_and_descriptions = get_nodes_and_descriptions(setclass, with_ldf, setheader, setnode, leafdesc, max_depth, leaf_id_column, leaf_desc_column)
        # populate the null LevelX_Node columns if the leaf_flag is true
        with_filled_null_values = fill_null_values(with_nodes_and_descriptions, max_depth, leaf_desc_column)
        
        # HIER_ID is the root of the hierarchy
        wide = with_filled_null_values.withColumn("hier_id", with_filled_null_values["path_array"][0])
        
        # join the SETCLST to get the descriptive column for SETCLASS
        joined = (
            wide
                .join(
                    setclst, 
                        wide["hier_type_id"] == setclst["SETCLASS"], 
                    how="left_outer"
                )
        )
        
        wide = joined.select(*wide.columns, "DESCRIPT")
        
        # transform the columns
        columns_to_drop = ["path", "path_array","parent_node_id", leaf_id_column, leaf_desc_column, "KOKRS", "SPRAS", "DATBI", "last_defined_level", "temp_node_seqnr", "sn", "cni", "node_seqnr", "leaf_seqnr", "is_root"]

        flattened = (
            wide
                .drop(*columns_to_drop)
                .distinct() 
                .withColumnRenamed("child_node_id","node_id") 
                .withColumnRenamed("DESCRIPT", "hier_type_text") 
                .withColumn("node_type", # define the NODE_TYPE - HIERARCHY_NODE if leaf_flag is false -
                        when(col("leaf_flag") == False, lit("HIERARCHY_NODE"))
                            .otherwise(
                                upper(
                                    expr(
                                        "regexp_replace(array_join(array_except(split(hier_type_text, ' '), array(reverse(split(hier_type_text, ' '))[0])), ' '), ' ', '_')",
                                    ) # remove the last word from the hier_type_text to define the NODE_TYPE if leaf_flag is true
                                )
                            )
                        )
                )
        
        key_cols = ["sub_key_id", "hier_id", "node_id", "node_type", "level"]
        static_cols = ["hier_type_id","hier_type_text","sub_key_id","hier_id", "node_id", "node_type","level"]
        dynamic_cols = [col for col in flattened.columns if col not in static_cols]
        etl_cols = ["__etl_keys_fprint", "__etl_effective_from", "__etl_effective_to", "__etl_is_active", "__etl_is_deleted"]
        with_etl_cols = flattened.selectExpr(
            "*",
            f"xxhash64(concat({', '.join(key_cols)})) AS __etl_keys_fprint",
            "current_date() AS __etl_effective_from",
            "CAST(NULL AS DATE) AS __etl_effective_to",
            "TRUE AS __etl_is_active",
            "FALSE AS __etl_is_deleted"
        )

        flattened = with_etl_cols.select(
            static_cols + dynamic_cols + etl_cols
        )
        
        return flattened
    
    def traverse_hierarchy(self, current_level: DataFrame, accumulated: DataFrame, all_nodes: DataFrame, path_node: str, custom_fields = []) -> DataFrame:
        while True:
            children = current_level.alias("p").join(
                all_nodes.alias("c"),
                on = [
                    col("p.hierarchy_id") == col("c.hierarchy_id"),
                    col("p.child") == col("c.parent")
                ],
                how = "inner"
            ).selectExpr(
                "c.child",
                "c.parent",
                "c.hierarchy_id",
                "c.node_id",
                "c.description",
                "c.leaf_description",
                "c.leaf_flag",
                *[f"c.{field} AS {field}" for field in custom_fields],
                "(p.level+1) AS level",
                f"concat(p.path,'>',c.{path_node}) AS path"
            )

            if children.isEmpty():
                break
            accumulated = accumulated.union(children).distinct()
            current_level = children
        return accumulated
    
    def get_max_depth(self, traversed: DataFrame) -> int:
        max_depth = traversed.selectExpr("max(level) as max_depth").collect()[0]["max_depth"]
        return max_depth
        
    def get_array_of_nodes(self, traversed: DataFrame) -> DataFrame:
        array_of_nodes = traversed.selectExpr("*", "split(path, '>') as path_array")
        return array_of_nodes
    
    def get_nodes_and_descriptions(self, nodes: DataFrame, max_depth: int, description_mapping: DataFrame) -> DataFrame:
        for i in range(max_depth):
            level_num = i + 1
            col_name = f"level_{level_num}_node"

            nodes = ( 
                nodes.alias("n")
                    .selectExpr(
                        "*",
                        f"COALESCE(path_array[{i}], path_array[{i}], '') AS {col_name}"
                    )
            )

        nodes_and_descriptions = nodes

        for i in range(max_depth):
            level_num = i + 1
            col_name = f"level_{level_num}_node"
            col_desc = f"level_{level_num}_node_text"

            nodes_and_descriptions = nodes_and_descriptions.alias("w").join(
                description_mapping.alias("dm"),
                    on = [
                        col("w.hierarchy_id") == col("dm.lookup_hierarchy_id"),
                        col(f"w.{col_name}") == col("dm.lookup_node")
                    ],
                    how = "left"
                    ).drop("lookup_hierarchy_id", "lookup_node").withColumnRenamed("lookup_description", col_desc)
        
        return nodes_and_descriptions
    
    def add_repeating_nodes_and_descriptions(self, max_depth: int, repeating_nodes_and_descriptions: DataFrame) -> DataFrame:
        for i in range(max_depth):
            col_name = f"level_{i + 1}_node"
            col_desc = f"level_{i + 1}_node_text"

            repeating_nodes_and_descriptions = (
                repeating_nodes_and_descriptions
                    .withColumn(
                        col_name,
                        when(
                            (col("leaf_flag") == True) & (i + 1 > col("level")),  # choose after the last defined level
                            expr("path_array[size(path_array) - 1]")  # use the last element of the path_array to populate the rest of the LevelX_Node columns
                        ).otherwise(
                            expr(f"COALESCE({col_name}, {col_name}, '')")  # keep the original value
                        )
                    )
            )

            # fill the description column for leaves
            repeating_nodes_and_descriptions = (
                repeating_nodes_and_descriptions
                    .withColumn(
                        col_desc,
                        when(
                            (col("leaf_flag") == True) & (i + 1 >= col("level")),
                            col("description")
                        ).otherwise(
                            expr(f"COALESCE({col_desc}, {col_desc}, '')")
                        )
                    )
            )
        return repeating_nodes_and_descriptions
    
    def transform_columns(self, key_columns: List[str], static_columns: List[str], hierarchy: DataFrame) -> DataFrame:
        dropped_columns = (
            hierarchy
                .drop(
                    "child", 
                    "parent",
                    "leaf_description",  
                    "path", 
                    "path_array"
                )
        )

        dynamic_columns = [col for col in dropped_columns.columns if col not in static_columns]
        etl_columns = ["__etl_keys_fprint", "__etl_effective_from", "__etl_effective_to", "__etl_is_active", "__etl_is_deleted"]

        add_etl_columns = (
            dropped_columns
                .selectExpr(
                    "*",
                    f"xxhash64(concat({', '.join(key_columns)})) AS __etl_keys_fprint",
                    "current_date() AS __etl_effective_from",
                    "CAST(NULL AS DATE) AS __etl_effective_to",
                    "TRUE AS __etl_is_active",
                    "FALSE AS __etl_is_deleted"
                )
        )


        hierarchy_flattened = add_etl_columns.select(
            static_columns + dynamic_columns + etl_columns
        )

        return hierarchy_flattened
    
    def hrrp_flattener(self, hierarchy_type: str) -> DataFrame:

        hrrp_node = spark.table(f"{env_vars.bronze_catalog}.fivetran_s4p.hrrp_node").filter(col("HRYTYPE") == f"{hierarchy_type}")
        hhrp_hierarchy_names = spark.table(f"{env_vars.bronze_catalog}.fivetran_s4p.hrrp_directoryt").filter(col("HRYTYPE") == f"{hierarchy_type}")
        descriptions = spark.table(f"{env_vars.bronze_catalog}.fivetran_s4p.hrrp_nodet").filter(col("HRYTYP") == f"{hierarchy_type}")

        leaf_config = {
            "CS15": (f"{env_vars.bronze_catalog}.sap_s4hana.fincs_fsitemt", "ITEM", "TXTMI"),
            "CS17": (f"{env_vars.bronze_catalog}.fivetran_s4p.fincs_bunitt", "BUNIT", "TXTMI"),
            "CS16": (f"{env_vars.bronze_catalog}.sap_s4hana.tf101", "ITEM", "TXTMI"),
            "CS21": (f"{env_vars.bronze_catalog}.sap_s4hana.tf101", "ITEM", "TXTMI"), 
        }

        leaf_desc_table_dir, leaf_id_column, leaf_desc_column = leaf_config[hierarchy_type]

        leaf_desc_table = spark.table(leaf_desc_table_dir)
        
        hierarchy = (
            hrrp_node.alias("n")
                .join(
                    leaf_desc_table.alias("f"),
                    how="left",
                    on=[
                        col("n.HRYNODE") == col(f"f.{leaf_id_column}"),
                    ]
                ).selectExpr(
                    "n.HRYID AS hierarchy_id",
                    "n.HRYNODE AS child",
                    "n.PARNODE AS parent",
                    f"f.{leaf_desc_column} AS description"
                )
        )


        leaf_flags = (
            hierarchy.alias("c")
                .join(
                    other=hierarchy.alias("p"),
                    on=[
                        col("c.child") == col("p.parent")
                    ],
                    how="left"
                )
                .selectExpr(
                    "c.*",
                    """
                    CASE
                        WHEN p.parent IS NULL THEN TRUE
                        ELSE FALSE
                    END AS leaf_flag
                    """
                )
        )

        all_nodes = (
            leaf_flags
                .selectExpr(
                    "child",
                    "parent",
                    "hierarchy_id",
                    "child AS node_id",
                    "description",
                    "'' AS leaf_description",
                    "leaf_flag"
                )
                .distinct()
        )

        roots = (
            all_nodes
                .selectExpr(
                    "*",
                    "1 AS level",
                    "child AS path"
            ).filter(col("parent") == '')
        )

        accumulated = roots
        current_level = roots

        traversed = self.traverse_hierarchy(accumulated, current_level, all_nodes, path_node = "child")
        
        max_depth = self.get_max_depth(traversed)
        nodes = self.get_array_of_nodes(traversed)
    
        description_mapping = (
            all_nodes.alias("a")
                .join(
                    descriptions.alias("d"),
                    how="left",
                    on=[
                        col("a.child") == col("d.HRYNODE"),
                        col("a.hierarchy_id") == col("d.HRYID")
                    ]
                ).join(
                    leaf_desc_table.alias("g"),
                    how="left",
                    on=[
                        col("a.child") == col(f"g.{leaf_id_column}")
                    ]
                ).selectExpr(
                    "a.child AS lookup_node",
                    "a.leaf_flag",
                    "a.hierarchy_id AS lookup_hierarchy_id",
                    f"""
                    CASE 
                        WHEN a.leaf_flag = FALSE 
                            THEN d.NODETXT 
                        ELSE g.{leaf_desc_column} 
                    END AS lookup_description
                    """
                ).drop("leaf_flag")
        )
        
        nodes_and_descriptions = self.get_nodes_and_descriptions(nodes, max_depth, description_mapping)
        repeating_nodes_and_descriptions = self.add_repeating_nodes_and_descriptions(max_depth, nodes_and_descriptions)

        hierarchy_name = (
            repeating_nodes_and_descriptions.alias("r")
                .join(
                    hhrp_hierarchy_names.alias("h"),
                    how="left",
                    on=[
                        col("h.HRYID") == col("r.hierarchy_id")
                    ]
                ).selectExpr(
                    "r.*",
                    "h.HRYTXT AS hierarchy_name"
                )
        )        

        key_columns = ["hierarchy_id", "node_id", "level"]
        static_columns = ["hierarchy_id", "hierarchy_name", "node_id", "description", "level", "leaf_flag"]        

        hrrp_hierarchy_flattened = self.transform_columns(key_columns, static_columns, hierarchy_name)
        
        return hrrp_hierarchy_flattened
    
    def vcodes_hierarchy_flattener(self, vcodes_flat_file: DataFrame) -> DataFrame:
        vcodes_h1 = (
            vcodes_flat_file
                .selectExpr(
                    "Child_Vcode AS child",
                    "Description AS description",
                    "Parent_Vcode_H1_REP AS parent",
                    "'REP' AS hierarchy_id"
                )
        )

        vcodes_h2 = (
            vcodes_flat_file
                .selectExpr(
                    "Child_Vcode AS child",
                    "Description AS description",
                    "Parent_Vcode_H2_BIZ AS parent",
                    "'BIZ' AS hierarchy_id"
                )
        )

        vcodes_h3 = (
            vcodes_flat_file
                .selectExpr(
                    "Child_Vcode AS child",
                    "Description AS description",
                    "Parent_Vcode_H3_BS AS parent",
                    "'BS' AS hierarchy_id"
                )
        )

        vcodes_h4 = (
            vcodes_flat_file
                .selectExpr(
                    "Child_Vcode AS child",
                    "Description AS description",
                    "Parent_Vcode_H4_MIP AS parent",
                    "'MIP' AS hierarchy_id"
                )
        )

        vcodes_h5 = (
            vcodes_flat_file
                .selectExpr(
                    "Child_Vcode AS child",
                    "Description AS description",
                    "Parent_Vcode_H5_BSMIP AS parent", 
                    "'BSMIP' AS hierarchy_id"
                )
        )

        vcodes_union = (
            vcodes_h1
                .union(vcodes_h2)
                .union(vcodes_h3)
                .union(vcodes_h4)
                .union(vcodes_h5)
                .dropDuplicates(["child", "parent", "hierarchy_id"])
        )

        vcodes_leaf_flags_all = (
            vcodes_union.alias("c")
                .join(
                    other=vcodes_union.alias("p"),
                    on=[
                        col("c.child") == col("p.parent")
                    ],
                    how="left"
                )
                .selectExpr(
                    "c.*",
                    """
                    CASE
                        WHEN p.parent IS NULL THEN TRUE
                        ELSE FALSE
                    END AS leaf_flag
                    """
                )
        )

        all_nodes = (
            vcodes_leaf_flags_all
                .selectExpr(
                    "c.child",
                    "c.parent",
                    "c.hierarchy_id",
                    "c.child AS node_id",
                    "c.description",
                    "c.description AS leaf_description",
                    "leaf_flag"
                )
                .distinct()
        )

        roots = (
            all_nodes
                .filter(
                    col("parent").isNull()
                )
        )

        # TODO: give this another name.... please don't re-use variable names, it makes debugging confusing!
        roots = roots.selectExpr(
            "*",
            "1 AS level",
            "child AS path"
        )

        accumulated = roots
        current_level = roots

        traversed = self.traverse_hierarchy(accumulated, current_level, all_nodes, path_node = "child")

        max_depth = self.get_max_depth(traversed)
        nodes = self.get_array_of_nodes(traversed)

        description_mapping = all_nodes.selectExpr(
            "child AS lookup_node",
            "hierarchy_id AS lookup_hierarchy_id",
            "leaf_description AS lookup_description"
            )
        
        nodes_and_descriptions = self.get_nodes_and_descriptions(nodes, max_depth, description_mapping)
        repeating_nodes_and_descriptions = self.add_repeating_nodes_and_descriptions(max_depth, nodes_and_descriptions)

        additional_ids = repeating_nodes_and_descriptions.selectExpr(
            "*",
            """
            CASE 
                WHEN hierarchy_id = 'REP' THEN 'Reporting'
                WHEN hierarchy_id = 'BIZ' THEN 'BIZ'
                WHEN hierarchy_id = 'BS' THEN 'Balance Sheet'
                WHEN hierarchy_id = 'MIP' THEN 'Management Information P&L'
                WHEN hierarchy_id = 'BSMIP' THEN 'Management Information P&L and Balance Sheet'
                ELSE hierarchy_id
            END AS hierarchy_name
            """
        )

        key_columns = ["hierarchy_id", "node_id", "level"]
        static_columns = ["hierarchy_id", "hierarchy_name", "node_id", "description", "level", "leaf_flag"]        

        vcodes_hierarchy_flattened = self.transform_columns(key_columns, static_columns, additional_ids)

        return vcodes_hierarchy_flattened
    
    def financial_statement_version_hierarchy_flattener(self,
                                                        financial_statement_items_in_structure: DataFrame, 
                                                        financial_statement_text_for_items: DataFrame, 
                                                        financial_statement_assignment_item_gl: DataFrame, 
                                                        financial_statement_version_description: DataFrame, 
                                                        gl_account_master: DataFrame, 
                                                        version_list: List[str]
                                                        ) -> DataFrame:

        filtered_financial_statement_items_in_structure = financial_statement_items_in_structure.filter(col('financial_statement_version').isin(version_list))
        filtered_financial_statement_text_for_items = financial_statement_text_for_items.filter(col('financial_statement_version').isin(version_list))
        filtered_financial_statement_assignment_item_gl = financial_statement_assignment_item_gl.filter(col('financial_statement_version').isin(version_list))

        hierarchy_descriptions = financial_statement_version_description.filter(col('language_key') == "E")

        fsv_hierachy = filtered_financial_statement_items_in_structure.alias("pc").join(
            filtered_financial_statement_text_for_items.alias("qt"),
            on=[
                col("pc.financial_statement_item") == col("qt.financial_statement_item"),
                col("pc.financial_statement_version") == col("qt.financial_statement_version")
            ],
            how = "left"
        ).join(
            hierarchy_descriptions.alias("ed"),
            on=[
                col("pc.financial_statement_version") == col("ed.financial_statement_version")
            ],
            how = "inner"
        ).selectExpr(
            "pc.financial_statement_version AS hierarchy_id",
            "pc.sequence_number as child",
            "pc.parent_id AS parent",
            "pc.financial_statement_item",
            "COALESCE(qt.financial_statement_item_description, qt.financial_statement_item_description, '') AS description",
            "ed.financial_statement_version_description AS hierarchy_name"
        )

        fsv_leaf_flags_all = (
           fsv_hierachy.alias("c")
                .join(
                    fsv_hierachy.alias("p"),
                    on=[
                        col("c.hierarchy_id") == col("p.hierarchy_id"),
                        col("c.child") == col("p.parent")
                    ],
                    how="left"
                ).selectExpr(
                    "c.*",
                    """
                    CASE 
                        WHEN p.parent IS NULL THEN TRUE 
                        ELSE FALSE 
                    END AS leaf_flag
                    """
                )
        )

        all_nodes = (
            fsv_leaf_flags_all
                .selectExpr(
                    "child",
                    "parent",
                    "hierarchy_id",
                    "financial_statement_item AS node_id",
                    "description",
                    "description AS leaf_description",
                    "leaf_flag",
                    "hierarchy_name"
                ).distinct()
        )
        
        roots = all_nodes.filter(col("hierarchy_id") == col("financial_statement_item"))

        roots = roots.selectExpr(
            "*",
            "1 AS level",
            "node_id AS path"
            )

        accumulated = roots
        current_level = roots

        custom_fields = ["hierarchy_name"]

        traversed = self.traverse_hierarchy(current_level, accumulated, all_nodes, "node_id", custom_fields)
        max_depth = self.get_max_depth(traversed)
        nodes = self.get_array_of_nodes(traversed)

        description_mapping = all_nodes.selectExpr(
            "hierarchy_id AS lookup_hierarchy_id",
            "node_id AS lookup_node",
            "description AS lookup_description"
            )
        
        nodes_and_descriptions = self.get_nodes_and_descriptions(nodes, max_depth, description_mapping)
        repeating_nodes_and_descriptions = self.add_repeating_nodes_and_descriptions(max_depth, nodes_and_descriptions)

        financial_statement_assignment_item_gl_expanded = (
            filtered_financial_statement_assignment_item_gl.alias("zc")
                .join(
                    gl_account_master.alias("m"),
                        (col("m.gl_account") >= col("zc.account_from")) & 
                        (col("m.gl_account") <= col("zc.account_to")), # join all the values between VONKT and BISKT
                    how="inner"
                ).selectExpr(
                    "zc.financial_statement_version",
                    "zc.financial_statement_item",
                    "zc.chart_of_accounts",
                    "m.gl_account"
                )
        )

        gl_accounts_expanded = (
            repeating_nodes_and_descriptions.alias("w")
                .join(
                    financial_statement_assignment_item_gl_expanded.alias("zc"),
                    on = [
                        col("w.node_id") == col("zc.financial_statement_item")
                    ],
                    how = "left"
                ).drop(
                    'node_id'
                ).selectExpr(
                    "w.*",
                    "COALESCE(zc.chart_of_accounts, zc.chart_of_accounts, '') AS chart_of_accounts",
                    "COALESCE(zc.gl_account, zc.gl_account, '') AS node_id",
                    "CASE WHEN w.leaf_flag = true THEN 'LEAF NODE' ELSE 'HIERARCHY NODE' END AS node_type"
                )
        )

        key_columns = ["hierarchy_id", "node_id", "chart_of_accounts", "level", "level_1_node", "level_2_node", "level_3_node", "level_4_node", "level_5_node", "level_6_node", "level_7_node", "level_8_node", "level_9_node"]
        static_columns = ["hierarchy_id", "hierarchy_name", "node_id", "chart_of_accounts", "node_type", "level", "leaf_flag"]

        fsv_hierarchy_flattened = self.transform_columns(key_columns, static_columns, gl_accounts_expanded)

        return fsv_hierarchy_flattened

# COMMAND ----------

hierarchy_flattener = HierarchyFlattener()