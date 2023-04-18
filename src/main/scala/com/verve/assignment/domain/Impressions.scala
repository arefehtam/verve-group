package com.verve.assignment.domain

case class Impressions(appId: String, advertiserId: Option[Int], countryCode: String, id: Option[String]) extends Event
