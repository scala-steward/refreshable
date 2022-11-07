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

import cats.arrow.FunctionK
import cats.syntax.all._
import cats.effect._
import cats.effect.testkit.TestControl
import retry._
import munit.CatsEffectSuite

import scala.concurrent.duration._

class RefreshableSuite extends CatsEffectSuite {

  trait RefreshableFactory {
    def resource[A](
        refresh: IO[A],
        cacheDuration: A => FiniteDuration,
        onRefreshFailure: PartialFunction[(Throwable, RetryDetails), IO[Unit]],
        onExhaustedRetries: PartialFunction[Throwable, IO[Unit]],
        onNewValue: Option[(A, FiniteDuration) => IO[Unit]] = None,
        defaultValue: Option[A] = None,
        retryPolicy: Option[RetryPolicy[IO]] = None
    ): Resource[IO, Refreshable[IO, A]]

  }

  def suite(factory: RefreshableFactory)(implicit loc: munit.Location): Unit = {

    test("Uses initial value if available") {
      factory
        .resource[Int](
          refresh = IO.pure(1),
          cacheDuration = _ => 1.second,
          onRefreshFailure = _ => IO.unit,
          onExhaustedRetries = _ => IO.unit,
          onNewValue = None,
          defaultValue = Some(2),
          retryPolicy = None
        )
        .use { r =>
          r.value.assertEquals(1)
        }
    }

    test("Retries on failure") {

      val cacheTTL = 2.seconds

      val run = IO.ref(0).flatMap { state =>
        factory
          .resource[Int](
            // Initial evaluation succeeds but first refresh will fail and need to be retried
            refresh = state.getAndUpdate(_ + 1).flatTap { curr =>
              IO.raiseError(Boom).whenA(curr == 1)
            },
            cacheDuration = _ => cacheTTL,
            onRefreshFailure = _ => IO.unit,
            onExhaustedRetries = _ => IO.unit,
            onNewValue = None,
            defaultValue = Some(2),
            retryPolicy = None
          )
          .use { r =>
            IO.sleep(3.seconds) >> r.get.assertEquals(CachedValue.Success(2))
          }
      }

      TestControl.executeEmbed(run)
    }

    test("Exhausted retries") {

      val cacheTTL = 1.seconds

      val run = IO.ref(0).flatMap { state =>
        factory
          .resource[Int](
            // Initial evaluation succeeds but all refreshes fail
            refresh = state.getAndUpdate(_ + 1).flatTap { curr =>
              IO.raiseError(Boom).whenA(curr > 0)
            },
            cacheDuration = _ => cacheTTL,
            onRefreshFailure = _ => IO.unit,
            onExhaustedRetries = _ => IO.unit,
            onNewValue = None,
            defaultValue = Some(2),
            retryPolicy = None
          )
          .use { r =>
            IO.sleep(cacheTTL * 5) >> r.get
              .assertEquals(CachedValue.Error(0, Boom))
          }
      }

      TestControl.executeEmbed(run)
    }

    test("Uses default value if construction fails") {
      factory
        .resource[Int](
          refresh = IO.raiseError(Boom),
          cacheDuration = _ => 1.second,
          onRefreshFailure = _ => IO.unit,
          onExhaustedRetries = _ => IO.unit,
          onNewValue = None,
          defaultValue = Some(2),
          retryPolicy = None
        )
        .use { r =>
          r.value.assertEquals(2)
        }
    }

    test("Throws if construction fails and no default value provided") {
      factory
        .resource[Int](
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

    test("Cancelation") {

      val cacheTTL = 1.second

      val run = IO.ref(0).flatMap { state =>
        factory
          .resource[Int](
            refresh = state.getAndUpdate(_ + 1),
            cacheDuration = _ => cacheTTL,
            onRefreshFailure = _ => IO.unit,
            onExhaustedRetries = _ => IO.unit,
            onNewValue = None,
            defaultValue = None,
            retryPolicy = None
          )
          .use { r =>
            r.cancel
              .assertEquals(true) >> r.get
              .assertEquals(CachedValue.Cancelled(0)) >>
              IO.sleep(cacheTTL * 2) >>
              // Check that the background fiber really is dead and not still refreshing
              r.get.assertEquals(CachedValue.Cancelled(0))
          }
      }

      TestControl.executeEmbed(run)
    }

    test("Cancelation race") {
      val run = factory
        .resource[Int](
          refresh = IO.pure(1),
          cacheDuration = _ => 1.second,
          onRefreshFailure = _ => IO.unit,
          onExhaustedRetries = _ => IO.unit,
          onNewValue = None,
          defaultValue = None,
          retryPolicy = None
        )
        .use { r =>
          r.cancel.both(r.cancel).flatMap { res =>
            IO(assert(Set(true -> false, false -> true).contains(clue(res))))
          }
        }

      run.replicateA_(100)
    }

    test("Restart when not canceled") {
      factory
        .resource[Int](
          refresh = IO.pure(1),
          cacheDuration = _ => 1.second,
          onRefreshFailure = _ => IO.unit,
          onExhaustedRetries = _ => IO.unit,
          onNewValue = None,
          defaultValue = None,
          retryPolicy = None
        )
        .use { r =>
          r.restart.assertEquals(false)
        }
    }

    test("Cancel then restart") {

      val cacheTTL = 1.second

      val run = factory
        .resource[Int](
          refresh = IO.pure(0),
          cacheDuration = _ => cacheTTL,
          onRefreshFailure = _ => IO.unit,
          onExhaustedRetries = _ => IO.unit,
          onNewValue = None,
          defaultValue = None,
          retryPolicy = None
        )
        .use { r =>
          r.cancel >>
            r.get.assertEquals(CachedValue.Cancelled(0)) >>
            r.restart.assertEquals(true) >> IO.sleep(cacheTTL * 2) >>
            r.get.assertEquals(CachedValue.Success(0))
        }

      TestControl.executeEmbed(run)
    }

    test("Restart race") {
      val run = factory
        .resource[Int](
          refresh = IO.pure(1),
          cacheDuration = _ => 1.second,
          onRefreshFailure = _ => IO.unit,
          onExhaustedRetries = _ => IO.unit,
          onNewValue = None,
          defaultValue = None,
          retryPolicy = None
        )
        .use { r =>
          r.cancel >> r.restart.both(r.restart).flatMap { res =>
            IO(assert(Set(true -> false, false -> true).contains(clue(res))))
          }
        }

      run.replicateA_(100)
    }

    test("onRefreshFailure is invoked if refresh fails") {

      val cacheTTL = 1.second

      val run = IO.ref(false).flatMap { ref =>
        factory
          .resource[Int](
            refresh = IO.raiseError(Boom),
            cacheDuration = _ => cacheTTL,
            onRefreshFailure = _ => ref.set(true),
            onExhaustedRetries = _ => IO.unit,
            onNewValue = None,
            defaultValue = Some(5),
            retryPolicy = None
          )
          .use { _ =>
            IO.sleep(cacheTTL * 2) >> ref.get.assertEquals(true)
          }
      }

      TestControl.executeEmbed(run)
    }

    test("onExhaustedRetries is invoked if refresh policy is exhausted") {

      val retryPeriod = 1.second

      val run = IO.ref(false).flatMap { ref =>
        factory
          .resource[Int](
            refresh = IO.raiseError(Boom),
            cacheDuration = _ => 5.millis,
            onRefreshFailure = _ => IO.unit,
            onExhaustedRetries = _ => ref.set(true),
            onNewValue = None,
            defaultValue = Some(5),
            retryPolicy = Some(
              RetryPolicies
                .constantDelay[IO](retryPeriod)
                .join(RetryPolicies.limitRetries(1))
            )
          )
          .use { _ =>
            IO.sleep(retryPeriod * 2) >> ref.get.assertEquals(true)
          }
      }

      TestControl.executeEmbed(run)

    }

    test("onNewValue is invoked with the expected values") {

      val run = IO.ref(0).flatMap { state =>
        IO.ref((0, 0.millis)).flatMap { result =>
          factory
            .resource[Int](
              refresh = state.getAndUpdate(_ + 1),
              cacheDuration = _ => 2.seconds,
              onRefreshFailure = _ => IO.unit,
              onExhaustedRetries = _ => IO.unit,
              onNewValue =
                Some((next: Int, d: FiniteDuration) => result.set(next -> d)),
              defaultValue = None,
              retryPolicy = None
            )
            .use { _ =>
              IO.sleep(3.seconds) >> result.get.assertEquals(1 -> 2.seconds)
            }
        }
      }

      TestControl.executeEmbed(run)
    }
  }

  suite(Default)
  suite(MapK)

  object Default extends RefreshableFactory {
    override def resource[A](
        refresh: IO[A],
        cacheDuration: A => FiniteDuration,
        onRefreshFailure: PartialFunction[(Throwable, RetryDetails), IO[Unit]],
        onExhaustedRetries: PartialFunction[Throwable, IO[Unit]],
        onNewValue: Option[(A, FiniteDuration) => IO[Unit]] = None,
        defaultValue: Option[A] = None,
        retryPolicy: Option[RetryPolicy[IO]] = None
    ): Resource[IO, Refreshable[IO, A]] = Refreshable.resource(
      refresh,
      cacheDuration,
      onRefreshFailure,
      onExhaustedRetries,
      onNewValue,
      defaultValue,
      retryPolicy
    )
  }

  object MapK extends RefreshableFactory {
    override def resource[A](
        refresh: IO[A],
        cacheDuration: A => FiniteDuration,
        onRefreshFailure: PartialFunction[(Throwable, RetryDetails), IO[Unit]],
        onExhaustedRetries: PartialFunction[Throwable, IO[Unit]],
        onNewValue: Option[(A, FiniteDuration) => IO[Unit]] = None,
        defaultValue: Option[A] = None,
        retryPolicy: Option[RetryPolicy[IO]] = None
    ): Resource[IO, Refreshable[IO, A]] = Refreshable
      .resource(
        refresh,
        cacheDuration,
        onRefreshFailure,
        onExhaustedRetries,
        onNewValue,
        defaultValue,
        retryPolicy
      )
      .map(_.mapK(FunctionK.id[IO]))
  }

  object Boom extends RuntimeException("BOOM")

}
