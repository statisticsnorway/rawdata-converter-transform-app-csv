package no.ssb.rawdata.converter.app.csv;


import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.ssb.avro.convert.core.FieldDescriptor;

@RequiredArgsConstructor
@Slf4j
public class AccountNoNormalizer {
    @NonNull
    private final String accountNoFieldName = "BrukerstedNavn";

    public String checkAndFix(FieldDescriptor field, String varValue) {
        if (accountNoFieldName.equals(field.getName()) && (varValue.contains("\n") || varValue.contains(System. lineSeparator())
                || varValue.contains("\t\n"))) {
            log.debug("Field value: {}", varValue);
            // TODO: do something with field, like:
            return varValue.replace("\n", "").replace("\t\n"," ").replace(System.lineSeparator(), " ");
        }
        ;
        return varValue;
    }
}