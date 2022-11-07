/*
 * Copyright 2022 Permutive
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.permutive.refreshable

import cats.effect._
import munit.CatsEffectSuite

import scala.concurrent.duration._

class RefreshableSuite extends CatsEffectSuite {

  def suite(implicit loc: munit.Location): Unit = {
    test("Uses default value if construction fails") {
      Refreshable
        .resource[IO, Int](
          refresh = IO.raiseError(Boom),
          cacheDuration = _ => 1.second,
          onRefreshFailure = _ => IO.unit,
          onExhaustedRetries = _ => IO.unit,
          onNewValue = None,
          defaultValue = Some(5),
          retryPolicy = None
        )
        .use { r =>
          r.value.assertEquals(5)
        }
    }

    test("Throws if construction fails and no default value provided") {
      Refreshable
        .resource[IO, Int](
          refresh = IO.raiseError(Boom),
          cacheDuration = _ => 1.second,
          onRefreshFailure = _ => IO.unit,
          onExhaustedRetries = _ => IO.unit,
          onNewValue = None,
          defaultValue = None,
          retryPolicy = None
        )
        .use_
        .intercept[Boom.type]
    }
  }

  suite

  object Boom extends RuntimeException("BOOM")

}
