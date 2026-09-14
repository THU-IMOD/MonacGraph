package db.monacgraph.retrieval.hipporag;

public final class Vectors {
    private Vectors() {}

    public static double cosine(float[] left, float[] right) {
        if (left == null || right == null || left.length != right.length || left.length == 0) {
            return 0;
        }
        double dot = 0;
        double leftNorm = 0;
        double rightNorm = 0;
        for (int i = 0; i < left.length; i++) {
            dot += left[i] * right[i];
            leftNorm += left[i] * left[i];
            rightNorm += right[i] * right[i];
        }
        if (leftNorm == 0 || rightNorm == 0) {
            return 0;
        }
        return dot / Math.sqrt(leftNorm * rightNorm);
    }

    public static double[] minMaxNormalize(double[] values) {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        for (double value : values) {
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        double[] normalized = new double[values.length];
        if (max <= min) {
            return normalized;
        }
        double span = max - min;
        for (int i = 0; i < values.length; i++) {
            normalized[i] = (values[i] - min) / span;
        }
        return normalized;
    }
}
