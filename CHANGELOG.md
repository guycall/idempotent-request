# Idempotent Request Changelog #

## v0.1.4 ##

* Fix an issue, when http response from backend != 200..226 caused lock to be taken

## v0.1.3 ##

* Fix an issue, when concurrent requests sent to an endpoint won't be protected by idempotency until the 1st request is finished

## v0.1.2 ##

* Fix rack response
* Disuse SETNX (@espadrine)

## v0.1.1 ##

* Add caching for successful responses (@gomayonqui)
