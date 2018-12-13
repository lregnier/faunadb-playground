import Extensions._
import faunadb.FaunaClient
import faunadb.query.{Expr, Select, _}
import faunadb.values.{RefV, Value}

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future

object DemoApp {
  val ApiKey = "???????????????????????????????????????"
  val DbName = "demo"

  val SpellsClassName = "spells"
  val SpellsClassIndexName = "spells_index"

}

object FaunaApi {

  // Databases
  def createDb(dbName: String)(implicit client: FaunaClient): Future[Value] = {
    client
      .query(
        Do(
          If(Exists(Database(dbName)), Delete(Database(dbName)), true),
          CreateDatabase(Obj("name" -> dbName))
        )
      )
  }

  def deleteDb(dbName: String)(implicit client: FaunaClient): Future[Value] = {
    client.query(
      If(Exists(Database(dbName)), Delete(Database(dbName)), Value(true))
    )
  }

  // Keys
  def createKey(dbName: String)(implicit client: FaunaClient): Future[Value] = {
    client
      .query(
        CreateKey(Obj("database" -> Database(dbName), "role" -> "server"))
      )
  }

  // Classes
  def createClass(className: String)(implicit client: FaunaClient): Future[Value] = {
    client.query(CreateClass(Obj("name" -> className)))
  }

  // Indexes
  def createIndex(className: String, indexName: String)(implicit client: FaunaClient): Future[Value] = {
    client.query(
      CreateIndex(
        Obj("name" -> indexName, "source" -> Class(className))
      )
    )
  }

  // Instances
  def createInstance(className: String, data: Expr)(implicit client: FaunaClient): Future[Value] = {
    client.query(
      Create(Class(Value(className)), Obj("data" -> data))
    )
  }

  def createAllInstances(className: String, data: Seq[Expr])(implicit client: FaunaClient): Future[Value] = {
    client.query(
      Map(
        Arr(data:_*),
        Lambda(
          Value("data"),
          Create(
            Class(Value(className)),
            Obj("data" -> Var("data"))
          )
        )
      )
    )
  }

  def readInstance(className: String, instanceId: String)(implicit client: FaunaClient): Future[Value] = {
    client.query(
      Select(Value("data"), Get(Ref(Class(className), Value(instanceId))))
    )
  }

  def readAllInstances(indexName: String)(implicit client: FaunaClient): Future[Value] = {
    client.query(
      Paginate(
        Match(Index(Value(indexName)))
      )
    )
  }

  def updateInstance(className: String, instanceId: String, data: Expr)(implicit client: FaunaClient): Future[Value] = {
    client.query(
      Update(
        Ref(Class(className), Value(instanceId)),
        Obj("data" -> data)
      )
    )
  }

  def replaceInstance(className: String, instanceId: String, data: Expr)(implicit client: FaunaClient): Future[Value] = {
    client.query(
      Replace(
        Ref(Class(className), Value(instanceId)),
        Obj("data" -> data))
    )
  }

  def deleteInstance(className: String, instanceId: String)(implicit client: FaunaClient): Future[Value] = {
    client.query(Delete(Ref(Class(className), Value(instanceId))))
  }

}

object Main extends App {
  import DemoApp._
  import FaunaApi._

  //---------- SETUP & ACCESS ----------//
  // Create admin client
  val adminClient = FaunaClient(DemoApp.ApiKey)

  // Create DB
  val createDbResult = createDb(DemoApp.DbName)(adminClient).await
  println(s"DB created: $createDbResult")

  // Create Key
  val createKeyResult = createKey(DemoApp.DbName)(adminClient).await
  val key = createKeyResult("secret").to[String].get

  // Create session client
  implicit val sessionClient = adminClient.sessionClient(key)


  //---------- SCHEMA & INDEXES ----------//
  // Create Class
  val createClassResult = createClass(SpellsClassName).await
  println(s"Class created: $createClassResult")

  // Create Class Index
  val createIndexResult = createIndex(SpellsClassName, SpellsClassIndexName).await
  println(s"Index created: $createIndexResult")


  //---------- CRUD OPERATIONS ----------//
  // Create Instance
  val createInstanceResult = createInstance(SpellsClassName, Obj("name" -> "Fire Beak", "element" -> "water", "cost" -> 15)).await
  println(s"Instance created: $createInstanceResult")

  // Read Instance
  val instanceId = createInstanceResult("ref").to[RefV].get.id
  val readInstanceResult = readInstance(SpellsClassName, instanceId).await
  println(s"Instance retrieved: $readInstanceResult")

  // Update Instance
  val updateInstanceResult = updateInstance(SpellsClassName, instanceId, Obj("cost" -> 20)).await
  println(s"Instance updated: $updateInstanceResult")

  // Delete Instance
  val deleteInstanceResult = deleteInstance(SpellsClassName, instanceId).await
  println(s"Instance deleted: $deleteInstanceResult")


  //---------- MISC OPERATIONS ----------//
  // Create several Instances
  val createAllInstancesResult = createAllInstances(SpellsClassName, Seq(
    Obj("name" -> "Water Dragon's Claw", "element" -> "water"),
    Obj("name" -> "Hippo's Wallow", "element" -> "water", "cost" -> 35))).await

  println(s"Instances created: $createAllInstancesResult")

  // Read Instances from Index
  val readAllInstancesResult = readAllInstances(SpellsClassIndexName).await
  println(readAllInstancesResult)

  // Replace Instance
  val instanceToReplace = createInstance(SpellsClassName, Obj("name" -> "Fire Beak", "element" -> "water", "cost" -> 15)).await
  val instanceToReplaceId = instanceToReplace("ref").to[RefV].get.id
  val replaceInstanceResult = replaceInstance(SpellsClassName, instanceToReplaceId, Obj("name" -> "Fire Beak", "element" -> "fire")).await
  println(s"Instance replaced: $replaceInstanceResult")


  //---------- CLEAN-UP & SHUTDOWN ----------//
  // Delete Db
  deleteDb(DbName)(adminClient).await

  // Close connections
  sessionClient.close()
  adminClient.close()

}
