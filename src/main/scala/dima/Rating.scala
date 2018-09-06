package dima

import dima.Utils.{ItemId, UserId}
import org.joda.time.DateTime

case class Rating(key: Int, user: UserId, item: ItemId, rating: Int, timestamp: DateTime, userPartition: Int,
                  itemPartition: Int)
