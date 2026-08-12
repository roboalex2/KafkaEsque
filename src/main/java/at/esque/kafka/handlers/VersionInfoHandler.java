package at.esque.kafka.handlers;

import at.esque.kafka.alerts.*;
import at.esque.kafka.alerts.model.UpdateDialogResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import javafx.application.HostServices;
import javafx.application.Platform;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

@Singleton
public class VersionInfoHandler {

    private static final String GITHUB_LATEST_RELEASE_URL = "https://api.github.com/repos/patschuh/KafkaEsque/releases/latest";
    private static final String TAG_NAME = "tag_name";
    private static final String HTML_URL = "html_url";
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    @Inject
    private ConfigHandler configHandler;

    private VersionInfo versionInfo;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final OkHttpClient httpClient = new OkHttpClient.Builder()
        .connectTimeout(Duration.ofSeconds(10))
        .readTimeout(Duration.ofSeconds(15))
        .build();

    public VersionInfoHandler() {
        objectMapper.disable(FAIL_ON_UNKNOWN_PROPERTIES);
        try (InputStream resourceAsStream = getClass().getResourceAsStream("/version.json")) {
            versionInfo = objectMapper.readValue(resourceAsStream, VersionInfo.class);
        } catch (Exception e) {
            Platform.runLater(() -> ErrorAlert.show(e));
        }
    }

    public VersionInfo getVersionInfo() {
        return versionInfo;
    }

    public UpdateInfo availableUpdate() {
        final Map<String, Object> latestVersion = checkLatestVersion();
        if (versionInfo == null) {
            return null;
        }
        final SemanticVersion currentVersionNumber = versionInfo.releaseVersion();
        if (latestVersion != null && currentVersionNumber != null) {
            final SemanticVersion latestVersionNumber = SemanticVersion.parse((String) latestVersion.get(TAG_NAME));
            final int i = currentVersionNumber.compareTo(latestVersionNumber);
            if (i < 0) {
                return new UpdateInfo((String) latestVersion.get(TAG_NAME), (String) latestVersion.get(HTML_URL));
            }
        }
        return null;
    }

    private Map<String, Object> checkLatestVersion() {
        if (Settings.isCheckForUpdatesEnabled(configHandler.getSettingsProperties())) {
            final Map<String, Object> versionCheckContent = configHandler.getVersionCheckContent();
            if (isVersionCacheExpired(versionCheckContent)) {
                Request request = new Request.Builder()
                    .url(GITHUB_LATEST_RELEASE_URL)
                    .addHeader("User-Agent", "patschuh/KafkaEsque")
                    .method("GET", null)
                    .build();


                Call call = httpClient.newCall(request);
                try (Response response = call.execute()) {

                    if (!response.isSuccessful()) {
                        return null;
                    }
                    if (response.body() == null) {
                        return null;
                    }
                    final Map<String, Object> responseRelease = objectMapper.readValue(response.body().byteStream(), MAP_TYPE);
                    Map<String, Object> checkContent = new HashMap<>();
                    checkContent.put("checkTime", Instant.now().toString());
                    checkContent.put("release", responseRelease);
                    configHandler.writeVersionCheckContent(checkContent);
                    return responseRelease;


                } catch (Exception e) {
                    Platform.runLater(() -> ErrorAlert.show("Update Check failed", "Failed to check for available updates", e.getMessage(), e, null, false));
                }
            } else {
                Object cachedRelease = versionCheckContent.get("release");
                return cachedRelease instanceof Map<?, ?> release
                    ? release.entrySet().stream().collect(java.util.stream.Collectors.toMap(
                        entry -> String.valueOf(entry.getKey()), Map.Entry::getValue))
                    : null;
            }
        }
        return null;
    }

    private boolean isVersionCacheExpired(Map<String, Object> versionCheckContent) {
        if (versionCheckContent == null) {
            return true;
        }
        try {
            Instant checkedAt = Instant.parse(String.valueOf(versionCheckContent.get("checkTime")));
            long cacheHours = Long.parseLong(configHandler.getSettingsProperties().get(Settings.CHECK_FOR_UPDATES_DURATION_BETWEEN_HOURS));
            return Duration.between(checkedAt, Instant.now()).toHours() > cacheHours;
        } catch (RuntimeException ignored) {
            return true;
        }
    }

    public void showDialogIfUpdateIsAvailable(HostServices hostServices) {
        if (isUpdateReminderActive()) {
            return;
        }
        Thread.ofVirtual().name("kafkaesque-version-check").start(() -> {
            try {
                UpdateInfo updateInfo = availableUpdate();
                if (updateInfo != null) {
                    Platform.runLater(() -> showUpdateDialog(updateInfo, hostServices));
                }
            } catch (Exception e) {
                Platform.runLater(() -> ErrorAlert.show("Update Check failed", "Failed to check for available updates", e.getMessage(), e, null, false));
            }
        });
    }

    private boolean isUpdateReminderActive() {
        return updateReminderTimestamp() > Instant.now().getEpochSecond();
    }

    private long updateReminderTimestamp() {
        return Optional.ofNullable(configHandler.getVersionCheckContent())
            .map(el -> el.get("showUpdateDialogAgainTimestamp"))
            .filter(Number.class::isInstance)
            .map(Number.class::cast)
            .map(Number::longValue)
            .orElse(0L);
    }

    private void showUpdateDialog(UpdateInfo updateInfo, HostServices hostServices) {
        final String askLaterFieldName = "showUpdateDialogAgainTimestamp";
        if (isUpdateReminderActive()) {
            return;
        }

        final UpdateDialogResult action = UpdateAlert.show("Update Available", "Version " + updateInfo.getTag() + " is available", "Do you want to open the release page?");
        if (UpdateDialogResult.OPEN.equals(action)) {
            try {
                hostServices.showDocument(updateInfo.getReleasePage());
            } catch (Exception e) {
                ErrorAlert.show(e);
            }
        } else if (UpdateDialogResult.REMIND_LATER.equals(action)) {
            Map<String, Object> versionCheckContent = Optional.ofNullable(configHandler.getVersionCheckContent()).orElse(new HashMap<>());
            versionCheckContent.put(askLaterFieldName, Instant.now().plus(1, ChronoUnit.DAYS).getEpochSecond());
            configHandler.writeVersionCheckContent(versionCheckContent);
        }
    }
}
