package common

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.api.CuratorWatcher
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.{WatchedEvent, Watcher}
import org.apache.log4j.Logger


class zkWatch {

}

object zkWatch{

  private val log = Logger.getLogger(classOf[zkWatch])

  def main(args: Array[String]): Unit = {

    PersistantWatcher("localhost:2181")
    //oneTimeWatcher()

  }


  def PersistantWatcher(zkaddress:String) = {
    val retryPolicy = new ExponentialBackoffRetry(1000, Integer.MAX_VALUE)
    val curator = CuratorFrameworkFactory.newClient(zkaddress, retryPolicy)

    val path = "/yl"

    curator.start()

    if(curator.checkExists().forPath(path) == null){
      curator.create().creatingParentsIfNeeded().forPath(path)
    }

    curator.getZookeeperClient.blockUntilConnectedOrTimedOut

    System.out.println("the original data is " + new String(curator.getData.forPath(path)))

    val nodeCache = new NodeCache(curator, path)

    nodeCache.getListenable.addListener(new NodeCacheListener() {
      @throws[Exception]
      override def nodeChanged() = {
        val currentData = nodeCache.getCurrentData
        System.out.println("data change watchecreate()d, and current data = " + new String(currentData.getData))
      }
    })

    nodeCache.start()
    Thread.sleep(Integer.MAX_VALUE)

  }


  def oneTimeWatcher()={
    val retryPolicy = new ExponentialBackoffRetry(1000, Integer.MAX_VALUE)
    val curator = CuratorFrameworkFactory.newClient("localhost:2181", retryPolicy)
    curator.start()
    curator.getZookeeperClient.blockUntilConnectedOrTimedOut()
    val path = "/yl"

    curator.getData.usingWatcher(new CuratorWatcher() {
      @throws[Exception]
      override def process(event: WatchedEvent): Unit = {
        if (event.getType == Watcher.Event.EventType.NodeDataChanged) {
          println(path + " data change watched." + new String(curator.getData.forPath(path)))
        }
      }
    }).forPath(path)
    //Thread.sleep(Integer.MAX_VALUE)

  }


}
