package net.lazygun.experiment.reveno.demo.kotlin

import org.reveno.atp.core.Engine
import org.reveno.atp.utils.MapUtils

fun main(args: Array<String>) {
    Engine("data/multiple-views").apply {
        domain().transaction("NewCar") { t, u ->
            u.repo().store(t.id(), t.arg())
        }
        .uniqueIdFor(Car::class.java)
        .command()


        domain().viewMapper(Car::class.java, CarView::class.java) { id, car, context ->
            CarView(id, car.colour, car.fuel, car.mileage)
        }

        // Fails when the mapper below is uncommented - Reveno only allows a single view
        // mapping per domain class. If you specify a second, the first is replaced, and
        // when you try to query on the first view, you'll get null.
//        domain().viewMapper(Car::class.java, CarStringView::class.java) { id, car, context ->
//            CarStringView("${car.colour}:${car.fuel}:${car.mileage}")
//        }

        startup()
        val id: Long = executeSync("NewCar", MapUtils.map("car", Car(Colour.RED, Fuel.PETROL, 100)))
        println(query().find(CarView::class.java, id))
        println(query().select(CarView::class.java))
        shutdown()
    }
}

enum class Colour { RED, YELLOW, BLACK, WHITE }
enum class Fuel { PETROL, DIESEL }
data class Car(val colour: Colour, val fuel: Fuel, val mileage: Int)
data class CarView(val id: Long, val colour: Colour, val fuel: Fuel, val mileage: Int)
data class CarStringView(val car: String)