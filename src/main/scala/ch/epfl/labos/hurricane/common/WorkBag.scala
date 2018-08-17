package ch.epfl.labos.hurricane.common

import scala.collection.mutable

case class WorkBag(
                    ready: mutable.Set[String] = mutable.Set.empty,
                    running: mutable.Set[String] = mutable.Set.empty,
                    done: mutable.Set[String] = mutable.Set.empty)
