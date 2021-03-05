package org.apache.griffin.measure

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, PrivateMethodTester}

trait GriffinTestBase
    extends FlatSpec
    with BeforeAndAfterAll
    with PrivateMethodTester
    with Matchers
    with Loggable {}
