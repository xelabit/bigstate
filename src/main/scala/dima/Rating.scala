package dima

import dima.Utils.{ItemId, UserId}
import org.joda.time.DateTime

case class Rating(user: UserId, item: ItemId, rating: Int, timestamp: DateTime)
