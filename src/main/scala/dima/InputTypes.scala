package dima

import dima.Utils.{ItemId, UserId}
import org.joda.time.DateTime

object InputTypes {

  /**
    * Represents an element of a data stream.
    * @param key an artificial key which is precomputed from the murmur hash function used in Flink. That is used in
    *            order to guarantee that records, that have neighboring user ids (we will partition our stream on a user
    *            id), end up on a same computer node.
    *            @see [[dima.misc.KeyPartition]]
    * @param user a user id. It is used for normal computations of corresponding factor vectors during SGD.
    * @param item an item id.
    * @param rating a rating that a user has given to a particular item.
    * @param timestamp the time of the event.
    * @param userPartition an id used to define, to which stratum (along user axis) this rating belongs.
    * @param itemPartition an id used to define, to which stratum (along item axis) this rating belongs.
    */
  case class Rating(key: Int, user: UserId, item: ItemId, rating: Int, timestamp: Long, userPartition: Int,
                    itemPartition: Int, label: String, ingestionTime: Long)
}