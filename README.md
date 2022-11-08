# Refreshable

`Refreshable` lives in the [Typelevel](https://typelevel.org/) Scala ecosystem
and offers a `Refreshable` type that operates like a cache of size 1 with a
background fiber that periodically refreshes the stored value. Use it when you
have criticial data that needs to be cached and you would rather read stale data
in the event that refreshing the data fails.

``` scala
trait Refreshable[F[_], A] {

  /** Get the unwrapped value of `A`
    */
  def value: F[A] = get.map(_.value)

  /** Get the value of `A` wrapped in a status
    */
  def get: F[CachedValue[A]]

  /** Cancel refreshing
    */
  def cancel: F[Boolean]

  /** Restart refreshing
    */
  def restart: F[Boolean]
}

sealed trait CachedValue[A] {
  def value: A
}

object CachedValue {
  case class Success[A](value: A) extends CachedValue[A]
  case class Error[A](value: A, error: Throwable) extends CachedValue[A]
  case class Cancelled[A](value: A) extends CachedValue[A]
}
```
