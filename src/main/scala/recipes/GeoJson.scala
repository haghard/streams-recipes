package recipes

import com.esri.core.geometry.{Geometry, GeometryEngine, SpatialReference}
import spray.json._

class RichGeometry(val geometry: Geometry, val spatialReference: SpatialReference = SpatialReference.create(4326))
    extends Serializable {

  def area2D(): Double = geometry.calculateArea2D()

  def distance(other: Geometry): Double =
    GeometryEngine.distance(geometry, other, spatialReference)

  def contains(other: Geometry): Boolean =
    GeometryEngine.contains(geometry, other, spatialReference)

  def within(other: Geometry): Boolean =
    GeometryEngine.within(geometry, other, spatialReference)

  def overlaps(other: Geometry): Boolean =
    GeometryEngine.overlaps(geometry, other, spatialReference)

  def touches(other: Geometry): Boolean =
    GeometryEngine.touches(geometry, other, spatialReference)

  def crosses(other: Geometry): Boolean =
    GeometryEngine.crosses(geometry, other, spatialReference)

  def disjoint(other: Geometry): Boolean =
    GeometryEngine.disjoint(geometry, other, spatialReference)
}

/**
  * Helper object for implicitly creating RichGeometry wrappers
  * for a given Geometry instance.
  */
object RichGeometry extends Serializable {
  implicit def createRichGeometry(g: Geometry): RichGeometry =
    new RichGeometry(g)
}

case class Feature(id: Option[JsValue], properties: Map[String, JsValue], geometry: RichGeometry) {
  def apply(property: String) = properties(property)
  def get(property: String)   = properties.get(property)
}

case class Features(features: Array[Feature]) extends IndexedSeq[Feature] {
  def apply(index: Int) = features(index)
  def length            = features.length
}

case class GeometryCollection(geometries: Array[RichGeometry]) extends IndexedSeq[RichGeometry] {
  def apply(index: Int) = geometries(index)
  def length            = geometries.length
}

object GeoJsonProtocol extends DefaultJsonProtocol {

  implicit object RichGeometryJsonFormat extends RootJsonFormat[RichGeometry] {
    def write(g: RichGeometry) =
      GeometryEngine
        .geometryToGeoJson(g.spatialReference, g.geometry)
        .parseJson
    def read(value: JsValue) = {
      val mg = GeometryEngine
        .geometryFromGeoJson(value.compactPrint, 0, Geometry.Type.Unknown)
      new RichGeometry(mg.getGeometry, mg.getSpatialReference)
    }
  }

  implicit object FeatureJsonFormat extends RootJsonFormat[Feature] {
    def write(f: Feature) = {
      val buf = scala.collection.mutable.ArrayBuffer(
        "type"       → JsString("Feature"),
        "properties" → JsObject(f.properties),
        "geometry"   → f.geometry.toJson
      )
      f.id.foreach(v ⇒ buf += "id" → v)
      JsObject(buf.toMap)
    }

    def read(value: JsValue) = {
      val jso        = value.asJsObject
      val id         = jso.fields.get("id")
      val properties = jso.fields("properties").asJsObject.fields
      val geometry   = jso.fields("geometry").convertTo[RichGeometry]
      Feature(id, properties, geometry)
    }
  }

  implicit object FeatureCollectionJsonFormat extends RootJsonFormat[Features] {
    def write(fc: Features) =
      JsObject("type" → JsString("FeatureCollection"), "features" → JsArray(fc.features.map(_.toJson): _*))

    def read(value: JsValue) =
      Features(value.asJsObject.fields("features").convertTo[Array[Feature]])
  }

  implicit object GeometryCollectionJsonFormat extends RootJsonFormat[GeometryCollection] {
    def write(gc: GeometryCollection) =
      JsObject("type" → JsString("GeometryCollection"), "geometries" → JsArray(gc.geometries.map(_.toJson): _*))

    def read(value: JsValue) =
      GeometryCollection(value.asJsObject.fields("geometries").convertTo[Array[RichGeometry]])
  }
}
