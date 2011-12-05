package backchat
package borg
package tests


class ServiceRegistrySpec extends ZooKeeperSpecification {

  def is =
    "A ServiceRegistry should" ^
      "be able to operate under concurrent loads" ! pending ^
    end

  def specify = null


}