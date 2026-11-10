// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.pushdown;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * The XY extent of an ISO WKB geometry, and {@code duckdb.spatial/intersects_extent@1}
 * ({@code a && b}) with DuckDB core's exact semantics:
 *
 * <ul>
 *   <li>a geometry without vertices (any empty geometry) intersects nothing;</li>
 *   <li>a POINT whose coordinates are all NaN is empty and contributes no vertex (that applies
 *       to a POINT anywhere, including inside a MULTIPOINT or GEOMETRYCOLLECTION; a NaN vertex
 *       of a LINESTRING or POLYGON is still a vertex);</li>
 *   <li>a vertex that is NaN on an axis makes that axis unbounded for the geometry;</li>
 *   <li>bounds are inclusive and compared as exact doubles, infinities included;</li>
 *   <li>Z and M are read past and ignored.</li>
 * </ul>
 *
 * <p>Anything that is not well-formed ISO WKB (unknown byte order or type, EWKB flags such as
 * the SRID flag, truncation, trailing bytes) is rejected with a {@link FilterV2Exception}.
 */
final class WkbExtent {
    private static final int MAX_DEPTH = 128;

    private double minX = Double.POSITIVE_INFINITY;
    private double maxX = Double.NEGATIVE_INFINITY;
    private double minY = Double.POSITIVE_INFINITY;
    private double maxY = Double.NEGATIVE_INFINITY;
    private boolean empty = true;

    private WkbExtent() {}

    /**
     * {@code a && b}: whether the extents of two WKB geometries intersect.
     *
     * @return the result, or {@code null} when either side is {@code null}
     * @throws FilterV2Exception when a side is not a binary ISO WKB geometry
     */
    static Boolean intersects(Object a, Object b) {
        if (a == null || b == null) return null;
        WkbExtent left = of(a);
        WkbExtent right = of(b);
        if (left.empty || right.empty) return false;
        return left.minX <= right.maxX && right.minX <= left.maxX
                && left.minY <= right.maxY && right.minY <= left.maxY;
    }

    static WkbExtent of(Object value) {
        if (!(value instanceof byte[] bytes)) {
            throw new FilterV2Exception("intersects_extent argument is not a WKB binary value");
        }
        WkbExtent extent = new WkbExtent();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        try {
            extent.geometry(buffer, 0);
        } catch (java.nio.BufferUnderflowException e) {
            throw new FilterV2Exception("truncated WKB geometry", e);
        }
        if (buffer.hasRemaining()) throw new FilterV2Exception("trailing bytes after WKB geometry");
        return extent;
    }

    private void geometry(ByteBuffer buffer, int depth) {
        if (depth > MAX_DEPTH) throw new FilterV2Exception("WKB geometry nests too deeply");
        byte order = buffer.get();
        if (order == 0) buffer.order(ByteOrder.BIG_ENDIAN);
        else if (order == 1) buffer.order(ByteOrder.LITTLE_ENDIAN);
        else throw new FilterV2Exception("invalid WKB byte order " + order);
        long type = Integer.toUnsignedLong(buffer.getInt());
        long base = type % 1000;
        long dimensions = type / 1000;
        if (base < 1 || base > 7 || dimensions > 3) {
            throw new FilterV2Exception("unsupported WKB geometry type " + type);
        }
        int width = 2 + (dimensions == 0 ? 0 : dimensions == 3 ? 2 : 1);
        switch ((int) base) {
            case 1 -> point(buffer, width);
            case 2 -> vertices(buffer, count(buffer, 8L * width), width);
            case 3 -> {
                for (int ring = count(buffer, 4); ring > 0; ring--) {
                    vertices(buffer, count(buffer, 8L * width), width);
                }
            }
            default -> {
                for (int part = count(buffer, 5); part > 0; part--) geometry(buffer, depth + 1);
            }
        }
    }

    /** Read an element count, rejecting one that cannot fit in the bytes left. */
    private static int count(ByteBuffer buffer, long minElementBytes) {
        long n = Integer.toUnsignedLong(buffer.getInt());
        if (n * minElementBytes > buffer.remaining()) throw new FilterV2Exception("truncated WKB geometry");
        return (int) n;
    }

    private void point(ByteBuffer buffer, int width) {
        double x = buffer.getDouble();
        double y = buffer.getDouble();
        boolean allNaN = Double.isNaN(x) && Double.isNaN(y);
        for (int i = 2; i < width; i++) allNaN &= Double.isNaN(buffer.getDouble());
        if (!allNaN) vertex(x, y);
    }

    private void vertices(ByteBuffer buffer, int n, int width) {
        for (int i = 0; i < n; i++) {
            double x = buffer.getDouble();
            double y = buffer.getDouble();
            for (int skip = 2; skip < width; skip++) buffer.getDouble();
            vertex(x, y);
        }
    }

    private void vertex(double x, double y) {
        empty = false;
        if (Double.isNaN(x)) {
            minX = Double.NEGATIVE_INFINITY;
            maxX = Double.POSITIVE_INFINITY;
        } else {
            if (x < minX) minX = x;
            if (x > maxX) maxX = x;
        }
        if (Double.isNaN(y)) {
            minY = Double.NEGATIVE_INFINITY;
            maxY = Double.POSITIVE_INFINITY;
        } else {
            if (y < minY) minY = y;
            if (y > maxY) maxY = y;
        }
    }
}
