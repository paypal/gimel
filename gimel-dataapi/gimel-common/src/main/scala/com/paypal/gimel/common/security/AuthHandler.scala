/*
 * Copyright 2018 PayPal Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.paypal.gimel.common.security

import java.net.URI

import scala.collection.immutable.{Iterable, Map, Seq}
import scala.collection.mutable.ListBuffer
import scala.sys.process._
import scala.util.control.Breaks._

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.gimelservices.GimelServiceUtilities
import com.paypal.gimel.common.gimelservices.payload.{PolicyDetails, PolicyItem}
import com.paypal.gimel.common.storageadmin.HDFSAdminClient
import com.paypal.gimel.logger.Logger

object AuthHandler {

  val logger = Logger(this.getClass)

  /**
    * This block allows entry for impersonation only if user is generic_user, in all other cases impersonation is not required.
    * Also, if GTS is started with spark.gimel.gts.impersonation.enabled=false, then this whole block must not execute.
    *
    * @param sparkSession Spark Session
    * @return Boolean, true indicates auth is required.
    */

  def isAuthRequired(sparkSession: SparkSession): Boolean = {

    val gtsDefaultUserOption = sparkSession.conf.getOption(GimelConstants.GTS_DEFAULT_USER_FLAG)
    // Commenting out assertion error when `spark.gimel.gts.default.user` is not defined
//    assert(gtsDefaultUserOption.isDefined,
//      s"Expecting the configuration: ${GimelConstants.GTS_DEFAULT_USER_FLAG} to be defined")
    val authRequired = gtsDefaultUserOption.isDefined && sparkSession.sparkContext.
  sparkUser.equalsIgnoreCase(gtsDefaultUserOption.get) && sparkSession
  .conf.get(GimelConstants.GTS_IMPERSONATION_FLAG, "true").toBoolean

    logger.info(s"GTS - AUTH REQUIRED : ${authRequired}")
    authRequired
  }

  /**
    * authenticateRangerPolicy - For a given  HDFS location and cluster ID, we will get the ranger policies from meta store
    * We will iterate the policyDetails and flatten all the policy items.
    * First we will do a group by the operation (read, write, execute)
    * Based on the operation, we will iterate the group by result and check for users match
    * if users not matched, we will get all the users if groups are available and match against those user groups
    * If ranger policies does not exist, we will use folder permissions to authenticate
    *
    * @param currentUser  - Current user for whom we need to authenticate
    * @param operation    - Read or write
    * @param hdfsLocation - Location of the hive table
    * @param dataSet      - data set name
    * @return
    */


  def authenticateRangerLocationPolicy(currentUser: String, operation: String, hdfsLocation: String, dataSet: String, clusterName: String, clusterNameNode: String, allPolicies: Map[String, Seq[PolicyDetails]]): Unit = {
    var canAccess = false
    val batchUsers = new ListBuffer[String]()
    val groupUsers = new ListBuffer[String]()
    logger.info(" authenticateRangerLocationPolicy args" + currentUser + ":" + operation + ":" + hdfsLocation + ":" + dataSet + ":" + clusterName + ":" + clusterNameNode)
    // The policies will not have the hdfs and cluster like hdfs://test_cluster/table...
    // It will have like /sys/user/tablename
    // So we need to strip the hdfs cluster name from it to send the correct location so that REST call won't fail.

    val hdfsPolicies: Seq[PolicyItem] = allPolicies.getOrElse("hdfs", Seq()).map(x => x.policyItems).flatten
    val accessTypes: scala.collection.immutable.Map[String, Seq[PolicyItem]] = hdfsPolicies.groupBy(x => x.accessTypes)

    if (accessTypes.size != 0) {
      accessTypes.foreach(
        accessType => {
          if (accessType._1.contains(operation)) {
            accessType._2.foreach(
              userGroups => if (userGroups.users.contains(currentUser)) canAccess = true
            )
          }
        }
      )
      logger.info("APACHE RANGER - Users access result " + canAccess)
      accessTypes.foreach(
        accessType => if (accessType._1.contains(operation)) {
          accessType._2.foreach(
            f = userGroup => {
              val userGroups = userGroup.groups.split(",").map(x => x.trim)
              userGroups.foreach(group => if (getUsersOfGroup(group).contains(currentUser)) canAccess = true)
              if (userGroup.users.length != 0) batchUsers += userGroup.users
              if (userGroup.groups.length != 0) groupUsers += userGroup.groups
            }
          )
        }
      )
      logger.info("APACHE RANGER - Groups access result " + canAccess)
    }

    // Ranger policies checked and user failed with Group/User policies
    if (!canAccess && accessTypes.size != 0) {
      val (accounts, roles) = getAccountsAndRoles(hdfsPolicies, allPolicies, operation, "hive")
      val errorMessage =
        s"""
           |[${currentUser}] does not have permission to [${operation}] ${dataSet} due to the access restriction
           |
           |To gain access to the dataset,
           |- You have to be a part of one of these accounts [${accounts}]
           |- Or, have one of the following roles [${roles}]
           |
           |""".stripMargin

      logger.info(errorMessage)
      throw new DataSetAccessException(errorMessage)
    }

    // Ranger policies checked and user passed with Group/User policies (OR) No Ranger policies exist
    // Do HDFS folder level authentication
    if (accessTypes.size == 0 || canAccess) {
      canAccess = checkForFolderPermission(hdfsLocation, currentUser, operation, clusterName, clusterNameNode)
      if (!canAccess) {
        val conf = new org.apache.hadoop.conf.Configuration()
        val clusterDataLocationPath = new java.net.URI(hdfsLocation).getPath()
        val newPath = getFolderThatExists(clusterDataLocationPath)
        val fs1 = FileSystem.get(new URI(newPath), conf)
        val hdfsPath = new Path(newPath)
        val fileStatus = fs1.getFileStatus(hdfsPath)
        val permission = fileStatus.getPermission
        val errorMessage =
          s"""
             |[${currentUser}] does not have permission to [${operation}] ${dataSet}
             |Current permission is ${permission} on the path [${hdfsLocation}]
             |And owner of the path is [${fileStatus.getOwner}]""".stripMargin
        throw new DataSetAccessException(errorMessage)
      }
    }

  }

  /**
    * To consutruct the error messages this function collects the accounts and roles that are needed to access the dataset
    * It goes through policyDetails and PolicyItem objects and finally return the accounts and roles in comma seperated string
    *
    * @param hdfsPolicies - Hdfs or hive policies that are there in the ranger
    * @param allPolicies  - This has the detailed policies
    * @param operation    - read or write  operation
    * @param storeType    - This can be called for getting table level or location level policies
    * @return - returns the accounts and roles in comma seperated string
    */
  def getAccountsAndRoles(hdfsPolicies: Seq[PolicyItem], allPolicies: Map[String, Seq[PolicyDetails]], operation: String, storeType: String): (String, String) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)
    logger.info("hdfshivePolicies => " + hdfsPolicies)
    logger.info("allPolicies => " + allPolicies)

    val hdfsUsersPolicies: Map[String, String] = hdfsPolicies.map { x => (x.accessTypes, x.users) }.toMap.filter(x => x._2.length != 0)
    val hdfsGroupPolicies: Map[String, String] = hdfsPolicies.map { x => (x.accessTypes, x.groups) }.toMap.filter(x => x._2.length != 0)
    val hivePolicies: Seq[PolicyItem] = allPolicies.getOrElse(storeType, Seq()).map(x => x.policyItems).flatten
    val hiveUsersPolicies: Map[String, String] = hivePolicies.map { x => (x.accessTypes, x.users) }.toMap.filter(x => x._2.length != 0)
    val hiveGroupPolicies: Map[String, String] = hivePolicies.map { x => (x.accessTypes, x.groups) }.toMap.filter(x => x._2.length != 0)

    val hdfssplitWiseUsers: Map[String, Iterable[String]] = getAccessTypesUserOrGroups(hdfsUsersPolicies)
    val hdfsoperationUsersListOnly: Map[String, String] = hdfssplitWiseUsers.filter(x => x._1.contains(operation)).map(y => (y._1, y._2.toSeq.mkString(",")))

    val hdfssplitWiseGroups: Map[String, Iterable[String]] = getAccessTypesUserOrGroups(hdfsGroupPolicies)
    val hdfsOperationGroupsListOnly = hdfssplitWiseGroups.filter(x => x._1.contains(operation)).map(y => (y._1, y._2.toSeq.mkString(",")))

    val hivesplitWiseUsers: Map[String, Iterable[String]] = getAccessTypesUserOrGroups(hiveUsersPolicies)
    val hiveoperationUsersListOnly = hivesplitWiseUsers.filter(x => x._1.contains(operation)).map(y => (y._1, y._2.toSeq.mkString(",")))

    val hivesplitWiseGroups: Map[String, Iterable[String]] = getAccessTypesUserOrGroups(hiveGroupPolicies)
    val hiveOperationGroupsListOnly: Map[String, String] = hivesplitWiseGroups.filter(x => x._1.contains(operation)).map(y => (y._1, y._2.toSeq.mkString(",")))

    val accounts = (hdfsoperationUsersListOnly.map(x => x._2) ++ hiveoperationUsersListOnly.map(x => x._2)).mkString
    val roles = (hdfsOperationGroupsListOnly.map(x => x._2) ++ hiveOperationGroupsListOnly.map(x => x._2)).mkString
    logger.info("accounts =>" + accounts)
    logger.info("roles =>" + roles)
    (accounts, roles)

  }

  /**
    * This is an utitility function to get the individual access vs users/groups for a given map
    * Map("read, execute" -> "USERA") if given, This returns
    * Map("read" -> List("USERA"),
    * "execute" -> List("USERA")))
    *
    * @param policyMaps - incoming access vs users maps
    * @return - returns the individual access vs users maps
    */
  def getAccessTypesUserOrGroups(policyMaps: Map[String, String]): Map[String, Iterable[String]] = {
    val hdfssplitWiseUsers: Map[String, Iterable[String]] = policyMaps.map { x =>
      val typesAsString = x._1
      val u = x._2
      val typesSplit = typesAsString.split(",").map { x => x.trim() }
      val tu = typesSplit.map { t => (t, u) }
      tu
    }.flatten.groupBy(x => x._1).map { x =>
      (x._1, x._2.map(_._2))
    }
    hdfssplitWiseUsers
  }

  /**
    * This function validates both hive database/table level policies along with HDFS location level policies
    *
    * @param currentUser     - user
    * @param operation       - read/write operation
    * @param database        - DB the table belongs to
    * @param table           - table name
    * @param hdfsLocation    - Location of HDFS
    * @param clusterName     - cluster where the table resides
    * @param clusterNameNode - The Name node of the cluster
    * @param dataSet         - data set name
    */
  def authenticateRangerHiveTableAndLocationPolicy(currentUser: String, operation: String, database: String, table: String, hdfsLocation: String, clusterName: String, clusterNameNode: String, dataSet: String, options: Map[String, Any]): Unit = {
    logger.info(
      s"""
         |Incoming params for authenticateRangerHiveTableAndLocationPolicy -->
         |currentUser : ${currentUser}
         |operation : ${operation}
         |database : ${database}
         |table : ${table}
         |hdfsLocation : ${hdfsLocation}
         |clusterName : ${clusterName}
         |clusterNameNode : ${clusterNameNode}
         |dataSet: ${dataSet}
         |
      """.stripMargin
    )
    val allPolicies: Map[String, Seq[PolicyDetails]] = getAllPolicies(currentUser, operation, database, table, hdfsLocation, clusterName, clusterNameNode, dataSet, options)

    authenticateHiveTablePolicy(currentUser, operation, database, table, dataSet, clusterName, allPolicies)
    authenticateRangerLocationPolicy(currentUser, operation, hdfsLocation, dataSet, clusterName, clusterNameNode, allPolicies)
  }

  def getAllPolicies(currentUser: String, operation: String, database: String, table: String, hdfsLocation: String, clusterName: String, clusterNameNode: String, dataSet: String, options: Map[String, Any]): Map[String, Seq[PolicyDetails]] = {
    val servUtils = new GimelServiceUtilities(options.map { x => (x._1, x._2.toString)})
    val clusterInfo = servUtils.getClusterInfo(clusterName)
    val clusterID = clusterInfo.clusterId
    val rangerPoliciesHive: Seq[PolicyDetails] = servUtils.getRangerPoliciesByHive(database, table, GimelConstants.STORAGE_TYPE_HIVE, clusterID);
    // FIXME: This is just a patch - we need to be able to get Views underlying Hive table policies as well
    val rangerPoliciesHDFS = if (hdfsLocation.isEmpty) {
      val subLocation = hdfsLocation.substring(7, hdfsLocation.size)
      val newLocation = subLocation.substring(subLocation.indexOf('/'), subLocation.size)
      servUtils.getRangerPoliciesByLocation(newLocation, GimelConstants.HADDOP_FILE_SYSTEM, clusterID)
    } else Seq[PolicyDetails]()
    val allPolicies: Map[String, Seq[PolicyDetails]] = Map("hive" -> rangerPoliciesHive, "hdfs" -> rangerPoliciesHDFS)
    allPolicies
  }

  /**
    * This function validates against the Hive database/table level policies
    *
    * @param currentUser - user
    * @param operation   - read/write
    * @param database    - DB name
    * @param table       - table name
    * @param dataSet     - data set name
    * @param clusterName - cluster the data set belongs to
    */
  def authenticateHiveTablePolicy(currentUser: String, operation: String, database: String, table: String, dataSet: String, clusterName: String, rangerPolicies: Map[String, Seq[PolicyDetails]]): Unit = {
    var canAccess = false
    val hivePolicies: Seq[PolicyItem] = rangerPolicies.getOrElse("hive", Seq()).map(x => x.policyItems).flatten
    val accessTypes: scala.collection.immutable.Map[String, Seq[PolicyItem]] = hivePolicies.groupBy(x => x.accessTypes)

    accessTypes.size match {
      case 0 => {
        logger.info("APACHE RANGER  - NO TABLE LEVEL POLICIES")
        canAccess = true
      }
      case _ => accessTypes.foreach(
        x => {
          if (x._1.contains(operation)) {
            x._2.foreach(
              y => if (y.users.contains(currentUser)) canAccess = true
            )
          }
        }
      )
        logger.info("APACHE RANGER - TABLE LEVEL - Users access result " + canAccess)
        accessTypes.foreach(
          x => if (x._1.contains(operation)) {
            x._2.foreach(
              y => {
                if (getUsersOfGroup(y.groups).contains(currentUser)) canAccess = true
              }
            )
          }
        )
        logger.info("APACHE RANGER - TABLE LEVEL - Groups access result " + canAccess)
    }
    if (!canAccess) {
      val (accounts, roles) = getAccountsAndRoles(hivePolicies, rangerPolicies, operation, "hdfs")
      val errorMessage =
        s"""
           |[${currentUser}] does not have permission to [${operation}] ${dataSet} due to the access restriction on ${database}.${table}
           |
           |To gain access to the dataset,
           |- You have to be a part of one of these accounts [${accounts}]
           |- Or, have one of the following roles [${roles}]
           |
           |""".stripMargin

      logger.info(errorMessage)
      throw new DataSetAccessException(errorMessage)
    }
  }

  /**
    * This function validates against the Hbase policies
    *
    * @param currentUser       - user name
    * @param operation         - read/write
    * @param nameSpaceAndTable - Hbase namespace name
    * @param dataSet           - data set name
    * @param clusterName       - cluster the dataset belongs to
    */

  def authenticateHbasePolicy(currentUser: String, operation: String, nameSpaceAndTable: String, dataSet: String, clusterName: String, dataSetProps: Map[String, Any]): Unit = {
    var canAccess = false

    // Once we get the cluster name we need to use Rest API to get the cluster ID
    val servUtils = new GimelServiceUtilities(dataSetProps.map { x => (x._1, x._2.toString)})
    val clusterInfo = servUtils.getClusterInfo(clusterName)
    val clusterID = clusterInfo.clusterId

    // Once we have the cluster id and hdfs location we can use rest utility to get the ranger policies
    val rangerPolicies: Seq[PolicyDetails] = servUtils.getRangerPoliciesByHbaseTable(nameSpaceAndTable, GimelConstants.STORAGE_TYPE_HBASE, clusterID);
    val policyIts: Seq[PolicyItem] = rangerPolicies.map(x => x.policyItems).flatten
    val accessTypes: scala.collection.immutable.Map[String, Seq[PolicyItem]] = policyIts.groupBy(x => x.accessTypes)


    val batchUsers = new ListBuffer[String]()
    val groupUsers = new ListBuffer[String]()

    accessTypes.size match {
      case 0 => {
        logger.info("APACHE RANGER - NOTES - NO RANGER POLICIES")
        canAccess = true
      }
      case _ => accessTypes.foreach(
        x => {
          if (x._1.contains(operation)) {
            x._2.foreach(
              y => if (y.users.contains(currentUser)) canAccess = true
            )
          }
        }
      )
        logger.info("APACHE RANGER - Users access result " + canAccess)
        accessTypes.foreach(
          x => if (x._1.contains(operation)) {
            x._2.foreach(
              y => {
                if (getUsersOfGroup(y.groups).contains(currentUser)) canAccess = true
                if (y.users.length != 0) batchUsers += y.users
                if (y.groups.length != 0) groupUsers += y.groups
              }
            )
          }
        )
        logger.info("APACHE RANGER - Groups access result " + canAccess)
    }
    if (!canAccess) {
      val firstPart =
        s"""
           |[${currentUser}] does not have permission to [${operation}] ${dataSet} due to the ranger policy on ${nameSpaceAndTable}
           |To access the data -
           |You need to have access to one of these accounts [${batchUsers.mkString(",")}]""".stripMargin
      val secondPart = groupUsers.size match {
        case 0 => ""
        case _ => s""" OR you need to have the role[s] [${groupUsers.mkString(",")}]"""

      }
      val errorMessage = firstPart + secondPart
      logger.info(errorMessage)
      throw new DataSetAccessException(errorMessage)
    }
  }

  /**
    * checkForFolderPermission => For a given path, user and operation (read or write)
    * we will get the file status which has both owner and permission
    * First we will check whether the given user is the owner
    * If Not we will check the permission rwx-rw-rw against the operation supplied (read or write)
    * In the permission since we have USERS-GROUPS-OTHERS, We will match against the last item (OTHERS)  (Substring of 6,9) will give the last 3 chars which we need to match with read/write
    *
    * @param filePath    - HDFS path for which permission need to be checked
    * @param currentUser - user to be authenticated
    * @param operation   - read or write operation
    * @return - Boolean whether user has access for read or write
    */
  def checkForFolderPermission(filePath: String, currentUser: String, operation: String, custerName: String, clusterNameNode: String): Boolean = {
    var canAccess = false
    val conf = new org.apache.hadoop.conf.Configuration()
    val clusterDataLocationPath = new java.net.URI(filePath).getPath()
    val newPath = getFolderThatExists(clusterDataLocationPath)
    val fs1 = FileSystem.get(new URI(newPath), conf)
    val hdfsPath = new Path(newPath)
    val fileStatus = fs1.getFileStatus(hdfsPath)
    if (fileStatus.getOwner != currentUser) {
      val permission = fileStatus.getPermission
      if (permission.toString.substring(6, 9).contains(operation(0))) {
        logger.info("Authenticated through current folder permissions")
        canAccess = true
      }
    }
    else {
      canAccess = true
    }
    canAccess
  }


  /**
    * getUsersGroup will get all the individual users belong to a unix group account
    * It will use scala process system call "getent group 'group name'" to get the users of the group
    *
    * @param groupName - the unix group name
    * @return - Array of users belong to the unix group
    */
  def getUsersOfGroup(groupName: String): Array[String] = {
    logger.info("getUsersOfGroup =====>" + groupName)

    val command = s"""getent group ${groupName}"""
    val users = command !!
    val newUserSet = users.contains(",") match {
      case true => val usersSet = users.split(',')
        val firstItem = usersSet(0)
        // result of the getent group <groupname> => sample_user_group:*:57816:ROB,TOM,GARRY
        // The first item ROB needed a special handling to trim till the last colon and take the rest till first comma
        val fixedFirstItem = firstItem.substring(firstItem.lastIndexOf(':') + 1, firstItem.length)
        val newUserSet: Array[String] = usersSet.slice(1, usersSet.length) :+ fixedFirstItem.toString
        newUserSet
      case false => Array[String]()
    }
    newUserSet
  }

  /**
    * When user create tables they give folder names which will not exist and Hive creates those folders.
    * But we cant use the path given by the user to do authentication as those folders wont exist.
    * We trim folders from right one by one to see which one exists and return the one that exists.
    *
    * @param IncomingFolder - user sent folder
    * @return - the folder that exists in HDFS
    */
  def getFolderThatExists(IncomingFolder: String): String = {
    var tempFolder = IncomingFolder
    try {
      breakable {
        while (true) {
          val folderExist = HDFSAdminClient.hdfsFolderExists(tempFolder) match {
            case true => break()
            case _ => None
          }
          tempFolder = tempFolder.substring(0, tempFolder.lastIndexOf("/"))
        }
      }
    }
    catch {
      case ex: Throwable => {
        ex.printStackTrace()
        throw ex
      }
    }
    tempFolder
  }

}

case class DataSetAccessException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause)
