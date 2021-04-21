package no.ssb.rawdata.converter.app.csv;


import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.ssb.avro.convert.core.FieldDescriptor;

@RequiredArgsConstructor
@Slf4j
public class AccountNoNormalizer {
    @NonNull
    private final String accountNoFieldName;

    public String checkAndFix(FieldDescriptor field, String varValue) {
        if (accountNoFieldName.equals(field.getName()) && varValue != null && varValue.length() == 12) {
            // log.debug("Field value: {}", varValue);
            // TODO: do something with field, like:
            return varValue.substring(0, 11);
        }
        ;
        return varValue;
    }
}