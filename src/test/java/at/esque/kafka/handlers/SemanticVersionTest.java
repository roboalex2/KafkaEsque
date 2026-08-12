package at.esque.kafka.handlers;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SemanticVersionTest {

    @Test
    public void comparesReleaseVersions() {
        assertTrue(SemanticVersion.parse("v2.10.0").compareTo(SemanticVersion.parse("2.9.6")) > 0);
        assertEquals(0, SemanticVersion.parse("v2.9.6+build.1").compareTo(SemanticVersion.parse("2.9.6")));
    }

    @Test
    public void ordersPreReleasesBeforeFinalRelease() {
        assertTrue(SemanticVersion.parse("2.9.6-rc.1").compareTo(SemanticVersion.parse("2.9.6")) < 0);
        assertTrue(SemanticVersion.parse("2.9.6-rc.2").compareTo(SemanticVersion.parse("2.9.6-rc.10")) < 0);
    }

    @Test
    void handlesLargeNumericPreReleaseIdentifiersWithoutOverflow() {
        assertTrue(SemanticVersion.parse("2.9.6-99999999999999999999")
            .compareTo(SemanticVersion.parse("2.9.6-10")) > 0);
    }

    @Test
    void rejectsMalformedVersions() {
        assertThrows(IllegalArgumentException.class, () -> SemanticVersion.parse("2.x.0"));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersion.parse(""));
    }
}
