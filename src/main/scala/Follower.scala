import org.apache.spark._
import org.apache.spark.rdd.RDD 
import org.apache.spark.util.IntParam 
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators 
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


val conf = new SparkConf().setMaster("local[*").setAppName("pat")
val sc = new SparkContext(conf)

val file_path = "/home/masterraf21/Code/STI/pat-scala/data/tcx.txt"

val graph = GraphLoader.edgeListFile(sc,file_path).reverse

val nodeWithSuccessors: VertexRDD[Array[VertexId]] = graph.ops.collectNeighborIds(EdgeDirection.Out)

val successorGraph = Graph(nodeWithSuccessors, graph.edges)

val neighborNodes: VertexRDD[Set[VertexId]] = successorGraph.aggregateMessages[Set[VertexId]] (
        triplet => {
            Iterator((triplet.dstId, triplet.srcAttr.toSet))
        },
        (s1, s2) => s1 ++ s2
    ).mapValues[Set[VertexId]](
        (id: VertexId, neighbors: Set[VertexId]) => neighbors - id
    )

val neighborEdges = neighborNodes.flatMap[Edge[String]](
  {
    case (source: VertexId, allDests: Set[VertexId]) => {
      allDests.map((dest: VertexId) => Edge(source, dest, ""))
    }
  }
)

val neighborGraph = Graph(graph.vertices, neighborEdges)

neighborGraph.vertices.collect()