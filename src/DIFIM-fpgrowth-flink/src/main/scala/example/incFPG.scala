package incFPGrowth

import fpgrowth.Item
import helper.IOHelperFlink
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import pfp.{MyMap, PFPGrowth, ParallelCounting}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

object incFPG {
  def main(args: Array[String]) {

    println("INCfromFile:  STARTING FPGROWTH IN FLINK")

    //Get global variables for Flink and parameter parser
    val parameter = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val itemDelimiter = " "

    //Parse input parameter
    val input = parameter.get("input")
    val inc = parameter.get("inc")
    val iniR = parameter.get("iniR")
   // println(input)
    val minSupport = parameter.get("support").toDouble
    val numGroup = parameter.get("group")    //group是什么

    //println("input: " + input + " support: " + minSupport + " numGroup: " + numGroup)

    //input and support are required
    if (input == null || input == "" || minSupport == null) {
      println("Please indicate input file and support: --input inputFile --support minSupport")
      return
    }

    //For measuring running time
    val starTime = System.currentTimeMillis()
    val pfp = new PFPGrowth(env, minSupport.toDouble)

    //Set number of group
    if (numGroup != null && numGroup.toInt >=0 ) {
      pfp.numGroup = numGroup.toInt
    }
    import org.apache.flink.api.scala._
    //Run FLink FPGrowth
    val iniTrans = IOHelperFlink.MyreadInput(env, input, itemDelimiter) //input:String格式(文件地址),itemDelimiter为空格
    val incTrans = IOHelperFlink.MyreadInput(env, inc, itemDelimiter) //input:String格式,itemDelimiter初始化为空
    val iniT = iniTrans.groupBy(0).reduceGroup(ParallelCounting.MyGroupReduce)//(string,ListBuffer[Item]格式
    val incT = incTrans.groupBy(0).reduceGroup(ParallelCounting.MyGroupReduce)
    val F = IOHelperFlink.readIniR(env, iniR, " ")

    val incCount = incT.count() * minSupport

    //Preprocess phase
    val addTrans = incT.map(new RichMapFunction[Tuple2[String,ListBuffer[Item]],Tuple2[String,ListBuffer[Item]]]{
      private val serialVersionUID = 1L
      private var iniTrans: java.util.List[Tuple2[String, ListBuffer[Item]]] = null

      override def open(parameters: Configuration) = {
        super.open(parameters)
        this.iniTrans = getRuntimeContext().getBroadcastVariable[Tuple2[String, ListBuffer[Item]]]("ini-input")
      }

      override def map(arg0: Tuple2[String, ListBuffer[Item]]) = {
       // to be committed
      }
    })
      .withBroadcastSet(iniTrans,"ini-input").filter(x=>x._2.nonEmpty) //
    val addData = addTrans.map(x=> x._2)
    val addTcount = (addTrans.count() * minSupport).toLong
    val iniTcount = (iniT.count() * minSupport).toLong

    val delData = iniT.map(MyMap.DelDataMap).withBroadcastSet(incTrans,"inc-input").filter(x=>x._2.nonEmpty)

    // Incremental phase
   val frequentItemsets = pfp.run(addData)
    val f = frequentItemsets.map(MyMap.incMap)


    // Merge phase
    // intersect(F,f)
    val bothmid = frequentItemsets.map(new RichMapFunction[Tuple2[ListBuffer[Item],Int],Tuple2[ListBuffer[String],Int]] {
      private val serialVersionUID = 1L
      private var iniR:java.util.List[Tuple2[ListBuffer[String],Int]] = null

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        this.iniR = getRuntimeContext.getBroadcastVariable[Tuple2[ListBuffer[String],Int]]("F")
      }
      override def map(in: Tuple2[ListBuffer[Item],Int])= {
        // to be committed
      }
    })
      .withBroadcastSet(F,"F").filter(x=> x._1.nonEmpty)

    val both = bothmid.map(new RichMapFunction[Tuple2[ListBuffer[String],Int],Tuple2[ListBuffer[String],Int]]{
      private val serialVersionUID = 1L
      private var del: java.util.List[(String,ListBuffer[String])]= null

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        this.del = getRuntimeContext().getBroadcastVariable("del-trans")
      }
      override def map(in: (ListBuffer[String], Int)) = {
        //to be committed
      }
    })
        .withBroadcastSet(delData,"del-trans").filter(x=> x._2 >= incCount)


    //diff(F,f)
      val inFmid = F.map(new RichMapFunction[Tuple2[ListBuffer[String],Int],Tuple2[ListBuffer[String],Int]] {
        private val serialVersionUID = 1L
        private var both: java.util.List[(ListBuffer[String],Int)] = null
        override def open(parameters: Configuration): Unit = {
          super.open(parameters)
          this.both = getRuntimeContext().getBroadcastVariable[(ListBuffer[String],Int)]("both")
        }
        override def map(in: (ListBuffer[String], Int)): (ListBuffer[String], Int) = {
          //to be committed
        }
      }).withBroadcastSet(both,"both").filter(x=> x._1.nonEmpty)


    val inF = inFmid.map(
      new RichMapFunction[Tuple2[ListBuffer[String],Int],Tuple2[ListBuffer[String],Int]] {
        private val serialVersionUID = 1L
        private var incT:java.util.List[Tuple2[String,ListBuffer[Item]]] = null
        override def open(parameters: Configuration): Unit = {
          super.open(parameters)
          this.incT = getRuntimeContext().getBroadcastVariable[Tuple2[String,ListBuffer[Item]]]("incT")
        }
        override def map(in: Tuple2[ListBuffer[String],Int])= {
          //to be committed
        }
      }
    ).withBroadcastSet(addTrans,"incT").filter(x=> x._2 >= incCount)


    val FGlobal=inF.map(new RichMapFunction[Tuple2[ListBuffer[String],Int],Tuple2[ListBuffer[String],Int]] {
        private val serialVersionUID = 1L
        private var del: java.util.List[(String,ListBuffer[String])]= null
        override def open(parameters: Configuration): Unit = {
          super.open(parameters)
          this.del = getRuntimeContext().getBroadcastVariable("del-trans")
        }
        override def map(in: Tuple2[ListBuffer[String],Int]) = {
          //to be committed
        }
      })
      .withBroadcastSet(delData,"del-trans").filter(x=> x._1.nonEmpty && x._2 >= incCount)


    val infmid = frequentItemsets.map(MyMap.incMap)
      .map(new RichMapFunction[Tuple2[ListBuffer[String],Int],Tuple2[ListBuffer[String],Int]] {
      private val serialVersionUID = 1L
      private var both:java.util.List[(ListBuffer[String],Int)] = null
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        this.both = getRuntimeContext.getBroadcastVariable[(ListBuffer[String],Int)]("both")
      }
      override def map(in: (ListBuffer[String], Int)): (ListBuffer[String], Int) = {
        //to be committed
      }
    }).withBroadcastSet(both,"both").filter(x=> x._1.nonEmpty)


    val inf = infmid.map(
      new RichMapFunction[Tuple2[ListBuffer[String],Int],Tuple2[ListBuffer[String],Int]] {
        private val serialVersionUID = 1L
        private var iniT:java.util.List[Tuple2[String,ListBuffer[Item]]] = null
        override def open(parameters: Configuration): Unit = {
          super.open(parameters)
          this.iniT = getRuntimeContext().getBroadcastVariable[Tuple2[String,ListBuffer[Item]]]("iniT")
        }
        override def map(in: Tuple2[ListBuffer[String],Int])= {
          //to be committed
        }
      }).withBroadcastSet(iniT,"iniT").filter(x=> x._2 >= incCount)

    val fGlobal = inf.map(new RichMapFunction[Tuple2[ListBuffer[String],Int],Tuple2[ListBuffer[String],Int]] {
          private val serialVersionUID = 1L
          private var del: java.util.List[(String,ListBuffer[String])]= null
          override def open(parameters: Configuration): Unit = {
            super.open(parameters)
            this.del = getRuntimeContext().getBroadcastVariable("del-trans")
          }
          override def map(in: Tuple2[ListBuffer[String],Int]) = {
            //to be committed
          }
        }).withBroadcastSet(delData,"del-trans").filter(x=> x._1.nonEmpty && x._2 >= incCount)

    both.print()
    FGlobal.print()
    fGlobal.print()


    println("TIME: " + (System.currentTimeMillis() - starTime) / 1000.0)
  }
}
