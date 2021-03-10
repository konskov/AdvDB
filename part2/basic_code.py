from pyspark.sql import SparkSession
import sys, time

disabled = sys.argv[1]

spark = SparkSession.builder.appName('query1-sql').getOrCreate()

rules_list = ['PushProjectionThroughUnion','ReorderJoin','EliminateOuterJoin','PushDownPredicates','PushDownLeftSemiAntiJoin',
'PushLeftSemiLeftAntiThroughJoin','LimitPushDown','ColumnPruning','CollapseRepartition','CollapseProject','OptimizeWindowFunctions',
'CollapseWindow','CombineFilters','EliminateLimits','CombineUnions','OptimizeRepartition','TransposeWindow','NullPropagation','ConstantPropagation',
'FoldablePropagation','OptimizeIn','ConstantFolding','EliminateAggregateFilter','ReorderAssociativeOperator','LikeSimplification',
'BooleanSimplification','SimplifyConditionals','PushFoldableIntoBranches','RemoveDispensableExpressions','SimplifyBinaryComparison',
'ReplaceNullWithFalseInPredicate','SimplifyConditionalsInPredicate','PruneFilters','SimplifyCasts','SimplifyCaseConversionExpressions',
'RewriteCorrelatedScalarSubquery','EliminateSerialization','RemoveRedundantAliases','UnwrapCastInBinaryComparison',
'RemoveNoopOperators','OptimizeUpdateFields','SimplifyExtractValueOps','OptimizeCsvJsonExprs','CombineConcats']

if disabled == "Y":
    spark.conf.set("spark.sql.cbo.enabled", False)
    # spark.conf.set("spark.sql.optimizer.excludedRules", spark.sql.catalyst.optimizer.LikeSimplification)
    # spark.conf.set("spark.sql.adaptive.enabled", False)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
    # spark.conf.set("spark.sql.files.maxPartitionBytes", 134217728)

elif disabled == "N":
    # spark.conf.set("spark.sql.cbo.enabled", True)
    # spark.conf.set("spark.sql.adaptive.enabled", True)
    # spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
    pass
else: 
    raise Exception ("This setting is not available.")

df = spark.read.format("parquet") 
df1 = df.load("hdfs://master:9000/movies/ratings.parquet") 
df2 = df.load("hdfs://master:9000/movies/genres.parquet") 
df1.registerTempTable("ratings") 
df2.registerTempTable("movie_genres")
df3 = df.load("hdfs://master:9000/movies/movies.parquet") 
df3.registerTempTable("movies")

# sqlString = "select * from movies as m where m._c1 like '%flies' "
sqlString = \
"SELECT * " + \
"FROM " + \
" (SELECT * FROM movie_genres LIMIT 100) as g, " + \
" ratings as r " + \
"WHERE " + \
" r._c1 = g._c0"

t1 = time.time() 
spark.sql(sqlString).show() 
t2 = time.time() 
spark.sql(sqlString).explain(extended=True)

print(type(df2))
print("Time with choosing join type %s is %.4f sec."%("enabled" if disabled == 'N' else "disabled", t2-t1))
