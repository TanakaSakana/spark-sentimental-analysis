import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.log4j.Logger
import org.apache.log4j.Level

import collection.mutable.{ArrayBuffer, HashMap}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object Entry {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LSA").setMaster("local[*]")
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val rawData = sc.textFile("raw.txt")
    val tables = rawData.map {
      line =>
        val cols = line.split(" -> ")
        val appId = cols(0)
        val context = cols(1)
        (appId, context.split(" "))
    }
    val numDocs = tables.count()
   
    val dtf = docTermFreqs(tables.values)
    val docIds = tables.keys.zipWithIndex().map { case (key, value) => (value, key) }.collect().toMap
    dtf.cache()
   
    val termFreq = dtf.flatMap(_.keySet).map((_, 1)).reduceByKey(_ + _)

   
    val idfs = termFreq.map {
      case (term, count) => (term, math.log(numDocs.toDouble / count))
    }.collect().toMap

   
    val termIds = idfs.keys.zipWithIndex.toMap
    val idTerms = termIds.map { case (term, id) => (id -> term) }
    val bIdfs = sc.broadcast(idfs).value
    val bTermIds = sc.broadcast(termIds).value
   
	val vecs = buildIfIdfMatrix(dtf, bIdfs, bTermIds)
    val mat = new RowMatrix(vecs)
    val svd = mat.computeSVD(100, computeU = true)

    println("Singular values: " + svd.s)
	// Find the terms in the top concepts
	// Matrix V
    val topConceptTerms = topTermsInTopConcepts(svd, 10, 3,  idTerms)
	// Find the docs in the top concepts
	// Matrix U
    //val topConceptDocs = topDocsInTopConcepts(svd, 3, 3,  docIds)
	for(terms<- topConceptTerms){
		println("Concept terms: " + terms.map(_._1).mkString(", "))
		println()
	}
    /*for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
      println("Concept terms: " + terms.map(_._1).mkString(", "))
      println("Concept docs: " + docs.map(_._1).mkString(", "))
      println()
    }*/
    //dtf.take(10).foreach(println)
  }

  def docTermFreqs(lemmatized: RDD[Array[String]]):
  RDD[mutable.HashMap[String, Int]] = {
    val dtf = lemmatized.map(terms => {
      val termFreqs = terms.foldLeft(new mutable.HashMap[String, Int]) {
        (map, term) => {
          map += term -> (map.getOrElse(term, 0) + 1)
          map
        }
      }
      termFreqs
    })
    dtf
  }

  def buildIfIdfMatrix(termFreq: RDD[mutable.HashMap[String, Int]],
                       bIdfs: Map[String, Double],
                       bTermIds: Map[String, Int]) = {
    termFreq.map {
      tf =>
        val docTotalTerms = tf.values.sum
        val termScores = tf.filter {
          case (term, freq) => bTermIds.contains(term)
        }.map {
          case (term, freq) => (bTermIds(term),
            bIdfs(term) * freq / docTotalTerms)
        }.toSeq
        Vectors.sparse(bTermIds.size, termScores)
    }
  }

  def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
                            numTerms: Int, termIds: Map[Int, String]): Seq[Seq[(String, Double)]] = {
    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
    val arr = v.toArray
    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termWeights.sortBy(-_._1)
      topTerms += sorted.take(numTerms).map { case (score, id) => (termIds(id), score) }
    }
    topTerms
  }

  def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
                           numDocs: Int, docIds: Map[Long, String]): Seq[Seq[(String, Double)]] = {
    val u = svd.U
    val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
    for (i <- 0 until numConcepts) {
      val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId
      topDocs += docWeights.top(numDocs).map { 
		case (score, id) => (docIds(id), score) 
	  }
    }
    topDocs
  }
}