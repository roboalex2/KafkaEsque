package at.esque.kafka.handlers;

import java.util.List;
import java.util.Objects;

/** A small SemVer comparator used by the update checker without pulling Gradle into the application. */
public record SemanticVersion(int major, int minor, int patch, List<String> preRelease)
        implements Comparable<SemanticVersion> {

    public SemanticVersion {
        preRelease = List.copyOf(preRelease);
    }

    public static SemanticVersion parse(String value) {
        Objects.requireNonNull(value, "value");
        var normalized = value.strip();
        if (normalized.startsWith("v") || normalized.startsWith("V")) {
            normalized = normalized.substring(1);
        }
        var withoutBuildMetadata = normalized.split("\\+", 2)[0];
        var versionAndPreRelease = withoutBuildMetadata.split("-", 2);
        var numbers = versionAndPreRelease[0].split("\\.");
        if (numbers.length == 0 || numbers.length > 3 || normalized.isEmpty()) {
            throw new IllegalArgumentException("Invalid semantic version: " + value);
        }
        var numericParts = new int[]{0, 0, 0};
        for (int i = 0; i < numbers.length; i++) {
            if (!isNumeric(numbers[i])) {
                throw new IllegalArgumentException("Invalid semantic version: " + value);
            }
            numericParts[i] = Integer.parseInt(numbers[i]);
        }
        var preRelease = versionAndPreRelease.length == 1
                ? List.<String>of()
                : List.of(versionAndPreRelease[1].split("\\."));
        return new SemanticVersion(numericParts[0], numericParts[1], numericParts[2], preRelease);
    }

    @Override
    public int compareTo(SemanticVersion other) {
        int comparison = Integer.compare(major, other.major);
        if (comparison == 0) comparison = Integer.compare(minor, other.minor);
        if (comparison == 0) comparison = Integer.compare(patch, other.patch);
        if (comparison != 0) return comparison;
        if (preRelease.isEmpty()) return other.preRelease.isEmpty() ? 0 : 1;
        if (other.preRelease.isEmpty()) return -1;

        int commonLength = Math.min(preRelease.size(), other.preRelease.size());
        for (int i = 0; i < commonLength; i++) {
            comparison = compareIdentifier(preRelease.get(i), other.preRelease.get(i));
            if (comparison != 0) return comparison;
        }
        return Integer.compare(preRelease.size(), other.preRelease.size());
    }

    private static int compareIdentifier(String left, String right) {
        boolean leftNumeric = isNumeric(left);
        boolean rightNumeric = isNumeric(right);
        if (leftNumeric && rightNumeric) {
            String normalizedLeft = stripLeadingZeros(left);
            String normalizedRight = stripLeadingZeros(right);
            int lengthComparison = Integer.compare(normalizedLeft.length(), normalizedRight.length());
            return lengthComparison != 0 ? lengthComparison : normalizedLeft.compareTo(normalizedRight);
        }
        if (leftNumeric != rightNumeric) {
            return leftNumeric ? -1 : 1;
        }
        return left.compareTo(right);
    }

    private static boolean isNumeric(String value) {
        return !value.isEmpty() && value.chars().allMatch(Character::isDigit);
    }

    private static String stripLeadingZeros(String value) {
        String stripped = value.replaceFirst("^0+(?!$)", "");
        return stripped.isEmpty() ? "0" : stripped;
    }
}
