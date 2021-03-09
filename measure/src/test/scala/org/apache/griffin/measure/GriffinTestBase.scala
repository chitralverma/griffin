package org.apache.griffin.measure

import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should._

trait GriffinTestBase
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with PrivateMethodTester
    with Matchers
    with Loggable {}
