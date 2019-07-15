package sample.crud

import slick.jdbc.MySQLProfile.api._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object Main extends App {

  // The config string refers to mysqlDB that we defined in application.conf
  val db = Database.forConfig("mysqlDB")
  // represents the actual table on which we will be building queries on
  val peopleTable = TableQuery[People]

  val dropPeopleCmd = DBIO.seq(peopleTable.schema.drop)
  val initPeopleCmd = DBIO.seq(peopleTable.schema.create)

  def dropDB = {
    //do a drop followed by initialisePeople
    val dropFuture = Future {
      db.run(dropPeopleCmd)
    }
    //Attempt to drop the table, Await does not block here
    Await.result(dropFuture, Duration.Inf).andThen {
      case Success(_) => initialisePeople
      case Failure(error) => println("Dropping the table failed due to: " + error.getMessage)
        initialisePeople
    }
  }

  def initialisePeople = {
    //initialise people
    val setupFuture = Future {
      db.run(initPeopleCmd)
    }
    //once our DB has finished initializing we are ready to roll, Await does not block
    Await.result(setupFuture, Duration.Inf).andThen {
      case Success(_) => println("DB reset")
      case Failure(error) => println("Initialising the table failed due to: " + error.getMessage)
    }
  }

  def runQuery = {
    val insertPeople = Future {
      val query = peopleTable ++= Seq(
        (10, "Jack", "Wood", 36),
        (20, "Tim", "Brown", 24)
      )
      // insert into `PEOPLE` (`PER_FNAME`,`PER_LNAME`,`PER_AGE`)  values (?,?,?)
      println(query.statements.head) // would print out the query one line up
      db.run(query)
    }
    Await.result(insertPeople, Duration.Inf).andThen {
      case Success(_) => listPeople
      case Failure(error) => println("Welp! Something went wrong! " + error.getMessage)
    }
  }

  def listPeople = {
    val queryFuture = Future {
      // simple query that selects everything from People and prints them out
      db.run(peopleTable.result).map(_.foreach {
        case (id, fName, lName, age) => println(s" $id $fName $lName $age")
      })
    }
    Await.result(queryFuture, Duration.Inf).andThen {
      case Success(_) => db.close() //cleanup DB connection
      case Failure(error) => println("Listing people failed due to: " + error.getMessage)
    }
  }

  def search(param: String, field: String) = {
    val queryFuture = Future {
      db.run(peopleTable.result).andThen {
        case Success(value) =>
          field match {
            case "fName" => value.filter(_._2 == param).foreach(person => println(s"found ${person}"))
            case "lName" => value.filter(_._3 == param).foreach(person => println(s"found ${person}"))
            case "age" => value.filter(_._4 == param.toInt).foreach(person => println(s"found ${person}"))
            case _ => println("invalid search parameter")
          }
        case Failure(error) => println("Listing people failed due to: " + error.getMessage)
      }
      //db.run(peopleTable.result).map(_.filter( (id, name, lname, age) => println()))
    }
    Await.result(queryFuture, Duration.Inf).andThen {
      case Success(_) => //cleanup DB connection
      case Failure(error) => println("Listing people failed due to: " + error.getMessage)
    }
  }

  def delete(param: String, field: String) = {
    val forDeletion = peopleTable.filter({
      field match {
        case "fName" => _.fName === param
        case "lName" => _.lName === param
        case "age" => _.age === param.toInt
      }
    })
    val queryFuture = Future {
      db.run(forDeletion.delete).andThen {
        case Success(value) =>
        case Failure(error) => println("Listing people failed due to: " + error.getMessage)
      }
      //db.run(peopleTable.result).map(_.filter( (id, name, lname, age) => println()))
    }
    Await.result(queryFuture, Duration.Inf).andThen {
      case Success(_) =>  //cleanup DB connection
      case Failure(error) => println("Listing people failed due to: " + error.getMessage)
    }
  }

  def update(id: Int, param: String, field: String) = {
    val lookUp = for { person <- peopleTable if person.id === id } yield field match {
      case "fName" => person.fName
      case "lName" => person.lName
    }
    val toUpdate = lookUp.update(param)
    val queryFuture = Future {
      db.run(toUpdate).andThen {
        case Success(value) =>
        case Failure(error) => println("Listing people failed due to: " + error.getMessage)
      }
      //db.run(peopleTable.result).map(_.filter( (id, name, lname, age) => println()))
    }
    Await.result(queryFuture, Duration.Inf).andThen {
      case Success(_) =>  //cleanup DB connection
      case Failure(error) => println("Listing people failed due to: " + error.getMessage)
    }
  }

  def createPerson(fName: String, lName: String, age: Int): Unit = {
    val insertPerson = Future {
      val query = peopleTable ++= Seq(
        (0, fName, lName, age)
      )

      println(query.statements.head) // would print out the query one line up
      db.run(query)
    }

    Await.result(insertPerson, Duration.Inf).andThen {
      case Success(_) => println("Added a person")
      case Failure(error) => println("Welp! Something went wrong! " + error.getMessage)
    }
  }

  dropDB
  Thread.sleep(1000)
  createPerson("Josh", "Weapon", 1337)
  createPerson("Josh", "Weapon", 1338)
  createPerson("Josh", "Weapon", 1339)
  createPerson("Joe", "Weapon", 1340)
  createPerson("Jordan", "UltraWeapon", 10000000)
  Thread.sleep(1000)
  search("1338", "age")
  search("Joe", "fName")
  search("UltraWeapon", "lName")
  Thread.sleep(1000)
  update(1, "Valentin", "fName")
  update(1, "Valentin", "lName")
  Thread.sleep(1000)
  delete("Joe", "fName")
  Thread.sleep(1000)
  listPeople
  Thread.sleep(10000)

}