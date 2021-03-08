package org.apache.griffin.measure

import org.scalatest._

trait GriffinTestBase
    extends FlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with PrivateMethodTester
    with Matchers
    with Loggable {}
