package dev.sahreb.delivery;

import java.util.UUID;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DeadLetterController {
    private final DeadLetterInspector inspector;

    public DeadLetterController(DeadLetterInspector inspector) {
        this.inspector = inspector;
    }

    @GetMapping("/api/dead-letters")
    public DeadLetterInspector.Snapshot inspect(@RequestParam(required = false) UUID eventId,
            @RequestParam(defaultValue = "20") int limit) {
        return inspector.inspect(eventId, limit);
    }
}
