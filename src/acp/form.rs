//! Release 0.8.5: opencode form → ACP `elicitation/create` (form mode).
//!
//! Mirrors the official 2.0.22 adapter's `packages/cli/src/acp/elicitation.ts`
//! — representability gates, the JSON-schema translation and the accepted-
//! answer mapping. Pure functions over [`crate::dto`] types; the agent layer
//! (`acp/agent.rs`) owns the I/O: capability checks, `elicitation/create`,
//! `form/reply` and `form/cancel`.

use std::collections::BTreeMap;

use agent_client_protocol::schema::v1::{
    BooleanPropertySchema, ElicitationContentValue, ElicitationPropertySchema, ElicitationSchema,
    EnumOption, IntegerPropertySchema, MultiSelectPropertySchema, NumberPropertySchema,
    StringFormat, StringPropertySchema,
};
use serde_json::{Map, Value};

use crate::dto::{FormField, FormInfo, FormOption, FormStringField};

/// The only form flows the bridge elicits — official `ElicitedKind`
/// (`question` = the `forms.ask` tool; `websearch.provider`). Anything else
/// is cancelled so the asker never hangs.
const ELICITED_KINDS: &[&str] = &["question", "websearch.provider"];

/// Credential-looking key/title patterns — official
/// `/password|passphrase|secret|token|api[_-]?key|credential|private[_-]?key/i`.
/// Hand-rolled contains check (no regex dependency); the list is the exact
/// expansion of each regex literal branch (spaced forms like "api key" do
/// NOT match the official `[_-]?` — they elicit normally).
fn looks_credential(haystack: &str) -> bool {
    let haystack = haystack.to_ascii_lowercase();
    [
        "password",
        "passphrase",
        "secret",
        "token",
        "apikey",
        "api-key",
        "api_key",
        "credential",
        "privatekey",
        "private-key",
        "private_key",
    ]
    .iter()
    .any(|pattern| haystack.contains(pattern))
}

// ---------------------------------------------------------------------------
// Field accessors (the dto stays pure data)
// ---------------------------------------------------------------------------

impl FormField {
    fn key(&self) -> &str {
        match self {
            FormField::String(f) => &f.key,
            FormField::Number(f) => &f.key,
            FormField::Integer(f) => &f.key,
            FormField::Boolean(f) => &f.key,
            FormField::Multiselect(f) => &f.key,
            FormField::External(f) => &f.key,
        }
    }

    fn title(&self) -> Option<&str> {
        match self {
            FormField::String(f) => f.title.as_deref(),
            FormField::Number(f) => f.title.as_deref(),
            FormField::Integer(f) => f.title.as_deref(),
            FormField::Boolean(f) => f.title.as_deref(),
            FormField::Multiselect(f) => f.title.as_deref(),
            FormField::External(f) => f.title.as_deref(),
        }
    }

    fn description(&self) -> Option<&str> {
        match self {
            FormField::String(f) => f.description.as_deref(),
            FormField::Number(f) => f.description.as_deref(),
            FormField::Integer(f) => f.description.as_deref(),
            FormField::Boolean(f) => f.description.as_deref(),
            FormField::Multiselect(f) => f.description.as_deref(),
            FormField::External(f) => f.description.as_deref(),
        }
    }

    fn required(&self) -> bool {
        match self {
            FormField::String(f) => f.required.unwrap_or(false),
            FormField::Number(f) => f.required.unwrap_or(false),
            FormField::Integer(f) => f.required.unwrap_or(false),
            FormField::Boolean(f) => f.required.unwrap_or(false),
            FormField::Multiselect(f) => f.required.unwrap_or(false),
            FormField::External(_) => false,
        }
    }

    fn hidden(&self) -> bool {
        match self {
            FormField::String(f) => f.hidden.unwrap_or(false),
            FormField::Number(f) => f.hidden.unwrap_or(false),
            FormField::Integer(f) => f.hidden.unwrap_or(false),
            FormField::Boolean(f) => f.hidden.unwrap_or(false),
            FormField::Multiselect(f) => f.hidden.unwrap_or(false),
            FormField::External(_) => false,
        }
    }

    fn has_when(&self) -> bool {
        let when = match self {
            FormField::String(f) => f.when.as_ref(),
            FormField::Number(f) => f.when.as_ref(),
            FormField::Integer(f) => f.when.as_ref(),
            FormField::Boolean(f) => f.when.as_ref(),
            FormField::Multiselect(f) => f.when.as_ref(),
            FormField::External(_) => None,
        };
        when.is_some_and(|w| !w.is_empty())
    }

    /// The raw default as a JSON value (per-type type on the wire), used for
    /// the representability check (default within options) and hidden-field
    /// answering.
    fn default(&self) -> Option<Value> {
        match self {
            FormField::String(f) => f.default.clone().map(Value::String),
            FormField::Number(f) => f.default.map(Value::from),
            FormField::Integer(f) => f.default.map(Value::from),
            FormField::Boolean(f) => f.default.map(Value::from),
            FormField::Multiselect(f) => f
                .default
                .clone()
                .map(|d| Value::Array(d.into_iter().map(Value::String).collect())),
            FormField::External(_) => None,
        }
    }
}

/// `q<N>_custom` — the free-text companion property of an optioned field
/// (official `customKey`). Never collides with a real field: `representable`
/// rejects forms whose field keys include it.
fn custom_key(field: &FormField) -> String {
    format!("{}_custom", field.key())
}

/// Official `hasOptions`: every multiselect has options on the wire; a
/// string field only when the `options` key is PRESENT (`options: []` still
/// counts — official `field.options !== undefined`).
fn has_options(field: &FormField) -> bool {
    match field {
        FormField::String(f) => f.options.is_some(),
        FormField::Multiselect(_) => true,
        _ => false,
    }
}

fn options_of(field: &FormField) -> &[FormOption] {
    match field {
        FormField::String(f) => f.options.as_deref().unwrap_or(&[]),
        FormField::Multiselect(f) => &f.options,
        _ => &[],
    }
}

fn string_text(
    field: &FormField,
) -> (
    Option<StringFormat>,
    Option<u32>,
    Option<u32>,
    Option<String>,
) {
    match field {
        FormField::String(f) => {
            // Official `text()`: core rejects an empty string for a required
            // field, so the client is told it needs at least one character.
            let min_length = f.min_length.map(|n| n.min(u32::MAX as u64) as u32);
            let min_length = if f.required.unwrap_or(false) {
                Some(min_length.unwrap_or(0).max(1))
            } else {
                min_length
            };
            (
                f.format.as_deref().and_then(to_string_format),
                min_length,
                f.max_length.map(|n| n.min(u32::MAX as u64) as u32),
                f.pattern.clone(),
            )
        }
        _ => (None, None, None, None),
    }
}

fn to_string_format(format: &str) -> Option<StringFormat> {
    match format {
        "email" => Some(StringFormat::Email),
        "uri" => Some(StringFormat::Uri),
        "date" => Some(StringFormat::Date),
        "date-time" => Some(StringFormat::DateTime),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Representability + schema translation (official `requestedSchema`)
// ---------------------------------------------------------------------------

/// The form-mode schema for a form, or `None` when the form must be cancelled
/// instead: the client lacks form elicitation, the form is not from an
/// allowed flow or has a credential-looking field, or ACP cannot represent it
/// faithfully (external fields, `when` conditions, a hidden required field
/// without a default, a default outside a field's options, or a free-text
/// answer alongside options that must also satisfy `required` / item bounds).
/// Hidden fields are not asked; they are answered with their default.
///
/// Field-level rules (official `representable`):
/// - string/multiselect with options: default must be one of the option
///   values; `custom` requires a non-required field, no `_custom` key clash,
///   and (multiselect) no min/max item bounds.
/// - number/integer/boolean are always representable.
pub fn requested_schema(form: &FormInfo, form_elicitation: bool) -> Option<ElicitationSchema> {
    if !form_elicitation {
        return None;
    }
    let kind = form
        .metadata
        .as_ref()
        .and_then(|m| m.get("kind"))
        .and_then(Value::as_str)?;
    if !ELICITED_KINDS.contains(&kind) {
        return None;
    }
    if form
        .fields
        .iter()
        .any(|f| looks_credential(f.key()) || f.title().is_some_and(looks_credential))
    {
        return None;
    }
    let fields: Vec<&FormField> = form
        .fields
        .iter()
        .filter(|f| !matches!(f, FormField::External(_)))
        .collect();
    if fields.len() != form.fields.len() || fields.iter().any(|f| f.has_when()) {
        return None;
    }
    if fields
        .iter()
        .any(|f| f.hidden() && f.required() && f.default().is_none())
    {
        return None;
    }
    let keys: Vec<&str> = fields.iter().map(|f| f.key()).collect();
    let visible: Vec<&FormField> = fields.iter().copied().filter(|f| !f.hidden()).collect();
    if !visible.iter().all(|f| representable(f, &keys)) {
        return None;
    }
    let mut schema = ElicitationSchema::new();
    for field in visible {
        for (key, property) in properties(field) {
            schema = schema.property(key, property, field.required());
        }
    }
    Some(schema)
}

/// Official `representable` — the per-field gate described above.
fn representable(field: &FormField, keys: &[&str]) -> bool {
    if !matches!(field, FormField::String(_) | FormField::Multiselect(_)) {
        return true;
    }
    if !has_options(field) {
        return true;
    }
    let values: Vec<&str> = options_of(field).iter().map(|o| o.value.as_str()).collect();
    let defaults: Vec<&str> = match field {
        FormField::String(f) => f.default.iter().map(String::as_str).collect(),
        FormField::Multiselect(f) => f.default.iter().flatten().map(String::as_str).collect(),
        _ => Vec::new(),
    };
    if defaults.iter().any(|d| !values.contains(d)) {
        return false;
    }
    let custom = match field {
        FormField::String(f) => f.custom.unwrap_or(false),
        FormField::Multiselect(f) => f.custom.unwrap_or(false),
        _ => false,
    };
    if !custom {
        return true;
    }
    if field.required() || keys.contains(&custom_key(field).as_str()) {
        return false;
    }
    match field {
        FormField::String(_) => true,
        FormField::Multiselect(f) => f.min_items.is_none() && f.max_items.is_none(),
        _ => false,
    }
}

/// Official `properties` — one property per visible field, two for an
/// optioned `custom` field (`<key>` select + `<key>_custom` free text).
fn properties(field: &FormField) -> Vec<(String, ElicitationPropertySchema)> {
    let title = field.title().map(str::to_string);
    let description = field.description().map(str::to_string);
    match field {
        FormField::String(f) => {
            let (format, min_length, max_length, pattern) = string_text(field);
            if !has_options(field) {
                let mut prop = StringPropertySchema::new();
                if let Some(t) = &title {
                    prop = prop.title(t.clone());
                }
                if let Some(d) = &description {
                    prop = prop.description(d.clone());
                }
                if let Some(fmt) = format {
                    prop = prop.format(fmt);
                }
                if let Some(n) = min_length {
                    prop = prop.min_length(n);
                }
                if let Some(n) = max_length {
                    prop = prop.max_length(n);
                }
                if let Some(p) = pattern {
                    prop = prop.pattern(p);
                }
                if let Some(d) = &f.default {
                    prop = prop.default_value(d.clone());
                }
                return vec![(f.key.clone(), prop.into())];
            }
            let select = select_schema(field, title.clone(), description.clone());
            if f.custom.unwrap_or(false) {
                vec![
                    (f.key.clone(), select.into()),
                    other_property(field, title.clone(), "Type your own answer"),
                ]
            } else {
                vec![(f.key.clone(), select.into())]
            }
        }
        FormField::Multiselect(f) => {
            let select = MultiSelectPropertySchema::titled(
                options_of(field)
                    .iter()
                    .map(enum_option)
                    .collect::<Vec<EnumOption>>(),
            );
            let select = {
                let mut s = select;
                if let Some(t) = &title {
                    s = s.title(t.clone());
                }
                if let Some(d) = &description {
                    s = s.description(d.clone());
                }
                if let Some(n) = f.min_items {
                    s = s.min_items(n);
                }
                if let Some(n) = f.max_items {
                    s = s.max_items(n);
                }
                // Official: a required multiselect needs at least one item.
                if f.required.unwrap_or(false) {
                    s = s.min_items(Some(f.min_items.unwrap_or(0).max(1)));
                }
                if let Some(d) = &f.default {
                    s = s.default_value(d.clone());
                }
                s
            };
            if f.custom.unwrap_or(false) {
                vec![
                    (f.key.clone(), select.into()),
                    other_property(field, title.clone(), "Add your own answer"),
                ]
            } else {
                vec![(f.key.clone(), select.into())]
            }
        }
        FormField::Number(f) => {
            let mut prop = NumberPropertySchema::new();
            if let Some(t) = &title {
                prop = prop.title(t.clone());
            }
            if let Some(d) = &description {
                prop = prop.description(d.clone());
            }
            if let Some(n) = f.minimum {
                prop = prop.minimum(n);
            }
            if let Some(n) = f.maximum {
                prop = prop.maximum(n);
            }
            if let Some(d) = f.default {
                prop = prop.default_value(d);
            }
            vec![(f.key.clone(), prop.into())]
        }
        FormField::Integer(f) => {
            let mut prop = IntegerPropertySchema::new();
            if let Some(t) = &title {
                prop = prop.title(t.clone());
            }
            if let Some(d) = &description {
                prop = prop.description(d.clone());
            }
            if let Some(n) = f.minimum {
                prop = prop.minimum(n);
            }
            if let Some(n) = f.maximum {
                prop = prop.maximum(n);
            }
            if let Some(d) = f.default {
                prop = prop.default_value(d);
            }
            vec![(f.key.clone(), prop.into())]
        }
        FormField::Boolean(f) => {
            let mut prop = BooleanPropertySchema::new();
            if let Some(t) = &title {
                prop = prop.title(t.clone());
            }
            if let Some(d) = &description {
                prop = prop.description(d.clone());
            }
            if let Some(d) = f.default {
                prop = prop.default_value(d);
            }
            vec![(f.key.clone(), prop.into())]
        }
        FormField::External(_) => Vec::new(),
    }
}

/// A string property with `oneOf` options — official select translation
/// ({type, title, description, oneOf, default} — `text()` constraints do
/// NOT apply to selects, only to plain strings and `_custom` companions).
fn select_schema(
    field: &FormField,
    title: Option<String>,
    description: Option<String>,
) -> StringPropertySchema {
    let mut prop = StringPropertySchema::new().one_of(
        options_of(field)
            .iter()
            .map(enum_option)
            .collect::<Vec<EnumOption>>(),
    );
    if let Some(t) = title {
        prop = prop.title(t);
    }
    if let Some(d) = description {
        prop = prop.description(d);
    }
    if let FormField::String(FormStringField {
        default: Some(d), ..
    }) = field
    {
        prop = prop.default_value(d.clone());
    }
    prop
}

/// The `_custom` free-text companion — never required, never defaulted.
fn other_property(
    field: &FormField,
    title: Option<String>,
    description: &str,
) -> (String, ElicitationPropertySchema) {
    let mut prop = StringPropertySchema::new()
        .title(format!(
            "{} (other)",
            title.unwrap_or_else(|| field.key().to_string())
        ))
        .description(description);
    // Official `other()`: `text()` rides the companion but ONLY for a
    // string field (a multiselect's free text is unbounded).
    if matches!(field, FormField::String(_)) {
        let (format, min_length, max_length, pattern) = string_text(field);
        if let Some(fmt) = format {
            prop = prop.format(fmt);
        }
        if let Some(n) = min_length {
            prop = prop.min_length(n);
        }
        if let Some(n) = max_length {
            prop = prop.max_length(n);
        }
        if let Some(p) = pattern {
            prop = prop.pattern(p);
        }
    }
    (custom_key(field), prop.into())
}

fn enum_option(option: &FormOption) -> EnumOption {
    let mut out = EnumOption::new(option.value.clone(), option.label.clone());
    if let Some(d) = &option.description {
        out = out.description(d.clone());
    }
    out
}

// ---------------------------------------------------------------------------
// Answer mapping (official `answer` + `fieldAnswer`)
// ---------------------------------------------------------------------------

/// The reply answer for an ACCEPTED elicitation response — hidden fields
/// answer with their default, absent keys are skipped.
///
/// Always `Some`: declined/cancelled/invalid responses are decided by the
/// CALLER (the action match) and never reach this function.
///
/// Merging rules (official `fieldAnswer`):
/// - hidden fields are not asked — they answer with their default;
/// - `_custom` free text (non-blank) overrides a string select's value and
///   appends to a multiselect's selection;
/// - absent keys are skipped (nothing to answer with).
pub fn answer(
    form: &FormInfo,
    content: &BTreeMap<String, ElicitationContentValue>,
) -> Option<Map<String, Value>> {
    let content: Map<String, Value> = content
        .iter()
        .map(|(key, value)| (key.clone(), content_to_value(value)))
        .collect();
    let mut out = Map::new();
    for field in &form.fields {
        if let Some(value) = field_answer(field, &content) {
            out.insert(field.key().to_string(), value);
        }
    }
    Some(out)
}

fn content_to_value(value: &ElicitationContentValue) -> Value {
    match value {
        ElicitationContentValue::String(s) => Value::String(s.clone()),
        ElicitationContentValue::Integer(i) => Value::from(*i),
        ElicitationContentValue::Number(n) => Value::from(*n),
        ElicitationContentValue::Boolean(b) => Value::from(*b),
        ElicitationContentValue::StringArray(v) => {
            Value::Array(v.iter().map(|s| Value::String(s.clone())).collect())
        }
        // The enum is #[non_exhaustive] — future content kinds are not
        // answerable; the field is skipped (official decode rejects
        // unknown value shapes too).
        _ => Value::Null,
    }
}

fn field_answer(field: &FormField, content: &Map<String, Value>) -> Option<Value> {
    if field.hidden() {
        return field.default();
    }
    let value = content.get(field.key()).cloned();
    let custom_eligible = matches!(field, FormField::String(_) | FormField::Multiselect(_))
        && has_options(field)
        && match field {
            FormField::String(f) => f.custom.unwrap_or(false),
            FormField::Multiselect(f) => f.custom.unwrap_or(false),
            _ => false,
        };
    if !custom_eligible {
        return value;
    }
    let custom = content
        .get(&custom_key(field))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());
    match custom {
        // The free-text answer wins over the selection (official).
        Some(text) => match field {
            FormField::String(_) => Some(Value::String(text.to_string())),
            _ => {
                let mut items: Vec<Value> = match value {
                    Some(Value::Array(items)) => items,
                    _ => Vec::new(),
                };
                items.push(Value::String(text.to_string()));
                Some(Value::Array(items))
            }
        },
        None => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A `question`-kind form builder — the wire shapes the `question` tool
    /// actually emits (`{key,title,description,type,options?,custom?}`).
    fn question(fields: serde_json::Value) -> FormInfo {
        serde_json::from_value(json!({
            "id": "frm_test",
            "sessionID": "ses_test",
            "title": "Questions",
            "metadata": { "kind": "question", "tool": { "messageID": "msg_1", "id": "call_q" } },
            "fields": fields,
        }))
        .expect("question form parses")
    }

    fn one_field(field: serde_json::Value) -> FormInfo {
        question(json!([field]))
    }

    /// The JSON the schema serializes to on the wire (what Zed receives).
    fn schema_json(form: &FormInfo) -> Value {
        serde_json::to_value(requested_schema(form, true).expect("representable form")).unwrap()
    }

    /// The wire question shape: string + options + custom, then a
    /// multiselect + options + custom — the canonical two-field form.
    fn canonical_form() -> FormInfo {
        question(json!([
            {
                "key": "q0",
                "title": "Pick an option",
                "description": "Which one do you prefer?",
                "type": "string",
                "options": [
                    { "value": "fast", "label": "Fast", "description": "Quick path" },
                    { "value": "safe", "label": "Safe" },
                ],
                "custom": true,
            },
            {
                "key": "q1",
                "title": "Pick many",
                "type": "multiselect",
                "options": [
                    { "value": "x", "label": "X" },
                    { "value": "y", "label": "Y" },
                ],
                "custom": true,
            },
        ]))
    }

    #[test]
    fn canonical_question_form_translates_like_the_official_adapter() {
        let schema = schema_json(&canonical_form());
        assert_eq!(schema["type"], "object");
        // Properties are ordered by key (the SDK's BTreeMap — Zed renders
        // attributes in the same dictionary order).
        let props = schema["properties"].as_object().expect("properties");
        let keys: Vec<&str> = props.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["q0", "q0_custom", "q1", "q1_custom"]);
        assert!(schema.get("required").is_none(), "nothing is required");

        // q0: string select with oneOf + the official default-omission.
        let q0 = &props["q0"];
        assert_eq!(q0["type"], "string");
        assert_eq!(q0["title"], "Pick an option");
        assert_eq!(q0["description"], "Which one do you prefer?");
        let one_of = q0["oneOf"].as_array().expect("oneOf");
        assert_eq!(one_of.len(), 2);
        assert_eq!(one_of[0]["const"], "fast");
        assert_eq!(one_of[0]["title"], "Fast");
        assert_eq!(one_of[0]["description"], "Quick path");
        assert_eq!(one_of[1]["const"], "safe");
        assert_eq!(one_of[1]["title"], "Safe");
        assert!(one_of[1].get("description").is_none());

        // q0_custom: the official "(other)" free text.
        let custom = &props["q0_custom"];
        assert_eq!(custom["type"], "string");
        assert_eq!(custom["title"], "Pick an option (other)");
        assert_eq!(custom["description"], "Type your own answer");

        // q1: array select with anyOf items + min items (none of the bounds).
        let q1 = &props["q1"];
        assert_eq!(q1["type"], "array");
        assert_eq!(q1["title"], "Pick many");
        assert_eq!(q1["items"]["anyOf"][0]["const"], "x");
        assert_eq!(q1["items"]["anyOf"][1]["title"], "Y");
        let custom = &props["q1_custom"];
        assert_eq!(custom["title"], "Pick many (other)");
        assert_eq!(custom["description"], "Add your own answer");
    }

    #[test]
    fn plain_string_field_translates_with_contraints_and_default() {
        let form = one_field(json!({
            "key": "q0",
            "title": "Email",
            "type": "string",
            "format": "email",
            "minLength": 2,
            "required": true,
            "default": "a@b.c",
        }));
        let schema = schema_json(&form);
        let q0 = &schema["properties"]["q0"];
        assert_eq!(q0["type"], "string");
        assert_eq!(q0["format"], "email");
        // Required ⇒ the client is told at least one character (official
        // `text()`: max(old, 1)).
        assert_eq!(q0["minLength"], 2);
        assert_eq!(q0["default"], "a@b.c");
        assert_eq!(schema["required"], json!(["q0"]));
    }

    #[test]
    fn required_string_without_min_length_lifts_to_one() {
        let form = one_field(json!({
            "key": "q0",
            "type": "string",
            "required": true,
        }));
        let schema = schema_json(&form);
        let q0 = &schema["properties"]["q0"];
        assert_eq!(q0["minLength"], 1);
        assert_eq!(schema["required"], json!(["q0"]));
    }

    #[test]
    fn required_multiselect_lifts_min_items_to_one() {
        let form = one_field(json!({
            "key": "q0",
            "type": "multiselect",
            "required": true,
            "options": [{ "value": "a", "label": "A" }],
        }));
        let schema = schema_json(&form);
        assert_eq!(schema["properties"]["q0"]["minItems"], 1);
        assert_eq!(schema["required"], json!(["q0"]));
    }

    #[test]
    fn number_integer_boolean_fields_translate_verbatim() {
        let form = question(json!([
            { "key": "q0", "title": "n", "type": "number", "minimum": 0.5, "maximum": 9.5, "default": 1.5 },
            { "key": "q1", "title": "i", "type": "integer", "minimum": 1, "maximum": 3, "default": 2 },
            { "key": "q2", "title": "b", "type": "boolean", "default": true },
        ]));
        let schema = schema_json(&form);
        let p = &schema["properties"];
        assert_eq!(p["q0"]["type"], "number");
        assert_eq!(p["q0"]["minimum"], 0.5);
        assert_eq!(p["q0"]["maximum"], 9.5);
        assert_eq!(p["q0"]["default"], 1.5);
        assert_eq!(p["q1"]["type"], "integer");
        assert_eq!(p["q1"]["minimum"], 1);
        assert_eq!(p["q1"]["default"], 2);
        assert_eq!(p["q2"]["type"], "boolean");
        assert_eq!(p["q2"]["default"], true);
    }

    #[test]
    fn credential_keys_and_titles_are_rejected() {
        for bad in [
            json!({ "key": "api_key", "type": "string" }),
            json!({ "key": "password", "type": "string" }),
            json!({ "key": "q0", "title": "Private-Key", "type": "string" }),
            json!({ "key": "my credential 2", "type": "string" }),
            json!({ "key": "TOKEN", "type": "string" }),
            json!({ "key": "q0", "title": "passphrase", "type": "string" }),
        ] {
            let form = one_field(bad.clone());
            assert!(
                requested_schema(&form, true).is_none(),
                "credential-looking field must be rejected: {bad}"
            );
        }
    }

    #[test]
    fn credential_check_matches_the_official_regex_exactly() {
        // The spaced forms do NOT match the official `api[_-]?key` /
        // `private[_-]?key` literals — they elicit normally.
        for ok in [
            json!({ "key": "api key", "type": "string" }),
            json!({ "key": "q0", "title": "API key", "type": "string" }),
            json!({ "key": "q0", "title": "private key", "type": "string" }),
        ] {
            let form = one_field(ok.clone());
            assert!(
                requested_schema(&form, true).is_some(),
                "spaced credential spelling must elicit: {ok}"
            );
        }
        // Every literal branch of the official regex (case-insensitive
        // substring) is rejected.
        for bad in [
            "password",
            "passphrase",
            "secret",
            "token",
            "apikey",
            "api-key",
            "api_key",
            "credential",
            "privatekey",
            "private-key",
            "private_key",
        ] {
            let form = one_field(json!({ "key": bad, "type": "string" }));
            assert!(
                requested_schema(&form, true).is_none(),
                "credential literal `{bad}` must be rejected"
            );
            let form = one_field(json!({ "key": "q0", "title": bad, "type": "string" }));
            assert!(
                requested_schema(&form, true).is_none(),
                "credential title literal `{bad}` must be rejected"
            );
        }
    }

    #[test]
    fn empty_options_array_still_counts_as_having_options() {
        // `options: []` + custom → the select branch with its `_custom`
        // companion (official `options !== undefined`).
        let form =
            one_field(json!({ "key": "q0", "type": "string", "options": [], "custom": true }));
        let schema = schema_json(&form);
        let props = schema["properties"].as_object().expect("properties");
        let keys: Vec<&str> = props.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            vec!["q0", "q0_custom"],
            "two properties incl. the companion"
        );
        assert_eq!(
            props["q0"]["oneOf"].as_array().map(|a| a.len()),
            Some(0),
            "empty oneOf"
        );
        assert_eq!(props["q0_custom"]["type"], "string");
        // `options: []` + default → the default is not in the (empty)
        // option set — not representable.
        let form =
            one_field(json!({ "key": "q0", "type": "string", "options": [], "default": "x" }));
        assert!(requested_schema(&form, true).is_none());
        // Same for a multiselect with empty options.
        let form =
            one_field(json!({ "key": "q0", "type": "multiselect", "options": [], "custom": true }));
        let schema = schema_json(&form);
        let props = schema["properties"].as_object().expect("properties");
        let keys: Vec<&str> = props.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["q0", "q0_custom"]);
    }

    #[test]
    fn optioned_string_select_carries_no_text_constraints() {
        // Official select: {type, title, description, oneOf, default} —
        // `text()` (format/minLength/maxLength/pattern) is NOT expanded.
        // (custom + required are mutually exclusive — official
        // `representable` rejects the combination; test them separately.)
        let form = one_field(json!({
            "key": "q0",
            "title": "Pick",
            "type": "string",
            "format": "uri",
            "pattern": "^a",
            "minLength": 2,
            "required": true,
            "options": [{ "value": "a", "label": "A" }],
        }));
        let schema = schema_json(&form);
        let q0 = &schema["properties"]["q0"];
        assert_eq!(q0["type"], "string");
        assert!(q0.get("minLength").is_none(), "select carries no minLength");
        assert!(q0.get("format").is_none(), "select carries no format");
        assert!(q0.get("pattern").is_none(), "select carries no pattern");
        assert_eq!(q0["oneOf"][0]["const"], "a");
        assert_eq!(schema["required"], json!(["q0"]));

        // A custom select (not required): the select still lacks the
        // constraints, but the `_custom` COMPANION expands `text()` for a
        // string field (official `other()`: `...(type === "string" ?
        // text(field) : {})`).
        let form = one_field(json!({
            "key": "q1",
            "title": "Pick",
            "type": "string",
            "format": "uri",
            "pattern": "^a",
            "minLength": 2,
            "custom": true,
            "options": [{ "value": "a", "label": "A" }],
        }));
        let schema = schema_json(&form);
        assert!(schema["properties"]["q1"].get("minLength").is_none());
        assert!(schema["properties"]["q1"].get("format").is_none());
        let custom = &schema["properties"]["q1_custom"];
        assert_eq!(custom["format"], "uri");
        assert_eq!(custom["pattern"], "^a");
        assert_eq!(custom["minLength"], 2);
    }

    #[test]
    fn external_when_unknown_kind_and_hidden_required_are_rejected() {
        // external field.
        let form = question(json!([{ "key": "q0", "type": "external", "url": "https://x" }]));
        assert!(requested_schema(&form, true).is_none());
        // `when` condition (either side of the AND-gate).
        let form = one_field(json!({
            "key": "q0", "type": "string",
            "options": [{ "value": "a", "label": "A" }],
            "when": [{ "key": "q1", "op": "eq", "value": "x" }],
        }));
        assert!(requested_schema(&form, true).is_none());
        // hidden + required + no default.
        let form =
            one_field(json!({ "key": "q0", "type": "string", "hidden": true, "required": true }));
        assert!(requested_schema(&form, true).is_none());
        // hidden + required + default is fine (answered with the default).
        let form = one_field(json!({
            "key": "q0", "type": "string", "hidden": true, "required": true, "default": "v"
        }));
        let schema = schema_json(&form);
        assert!(
            schema.get("required").is_none(),
            "hidden fields do not surface"
        );
        assert!(schema["properties"].as_object().unwrap().is_empty());
    }

    #[test]
    fn unknown_kind_and_missing_capability_are_rejected() {
        let form = one_field(json!({ "key": "q0", "type": "string" }));
        // Missing client capability.
        assert!(requested_schema(&form, false).is_none());
        // Kind outside the whitelist (#52636-style metadata absent).
        let mut other = form.clone();
        other.metadata = Some(
            json!({ "kind": "auth", "tool": { "id": "t" } })
                .as_object()
                .unwrap()
                .clone(),
        );
        assert!(requested_schema(&other, true).is_none());
        let mut no_meta = form.clone();
        no_meta.metadata = None;
        assert!(requested_schema(&no_meta, true).is_none());
        // The websearch.provider kind IS elicited.
        let mut web = form.clone();
        web.metadata = Some(
            json!({ "kind": "websearch.provider" })
                .as_object()
                .unwrap()
                .clone(),
        );
        assert!(requested_schema(&web, true).is_some());
    }

    #[test]
    fn default_outside_options_is_rejected() {
        let form = one_field(json!({
            "key": "q0", "type": "string",
            "options": [{ "value": "a", "label": "A" }],
            "default": "b",
        }));
        assert!(requested_schema(&form, true).is_none());
        let form = one_field(json!({
            "key": "q0", "type": "multiselect",
            "options": [{ "value": "a", "label": "A" }],
            "default": ["a", "b"],
        }));
        assert!(requested_schema(&form, true).is_none());
    }

    #[test]
    fn custom_with_required_or_bounds_or_key_clash_is_rejected() {
        // custom + required.
        let form = one_field(json!({
            "key": "q0", "type": "string", "custom": true, "required": true,
            "options": [{ "value": "a", "label": "A" }],
        }));
        assert!(requested_schema(&form, true).is_none());
        // custom + multiselect bounds.
        let form = one_field(json!({
            "key": "q0", "type": "multiselect", "custom": true, "minItems": 1,
            "options": [{ "value": "a", "label": "A" }],
        }));
        assert!(requested_schema(&form, true).is_none());
        // `_custom` key clash with a real field.
        let form = question(json!([
            { "key": "q0_custom", "type": "string" },
            { "key": "q0", "type": "string", "custom": true, "options": [{ "value": "a", "label": "A" }] },
        ]));
        assert!(requested_schema(&form, true).is_none());
    }

    #[test]
    fn hidden_field_answers_with_its_default() {
        let form = question(json!([
            { "key": "q0", "type": "string", "hidden": true, "default": "answer" },
            { "key": "q1", "type": "string" },
        ]));
        let content = BTreeMap::from([(
            "q1".to_string(),
            ElicitationContentValue::String("hi".into()),
        )]);
        let answer = answer(&form, &content).expect("accepted content maps");
        assert_eq!(
            answer.get("q0"),
            Some(&json!("answer")),
            "hidden default fills in"
        );
        assert_eq!(answer.get("q1"), Some(&json!("hi")));
    }

    #[test]
    fn custom_free_text_overrides_string_and_appends_to_multiselect() {
        let form = canonical_form();
        let content = BTreeMap::from([
            (
                "q0".to_string(),
                ElicitationContentValue::String("fast".into()),
            ),
            (
                "q0_custom".to_string(),
                ElicitationContentValue::String("   ".into()),
            ),
            (
                "q1".to_string(),
                ElicitationContentValue::StringArray(vec!["x".into()]),
            ),
            (
                "q1_custom".to_string(),
                ElicitationContentValue::String("z".into()),
            ),
        ]);
        let answer0 = answer(&form, &content).expect("accepted content maps");
        // Blank custom text → the selection stands.
        assert_eq!(answer0.get("q0"), Some(&json!("fast")));
        // Multiselect: custom appends to the selection.
        assert_eq!(answer0.get("q1"), Some(&json!(["x", "z"])));
        // Non-blank custom text wins over the string selection.
        let content = BTreeMap::from([
            (
                "q0".to_string(),
                ElicitationContentValue::String("fast".into()),
            ),
            (
                "q0_custom".to_string(),
                ElicitationContentValue::String("custom".into()),
            ),
        ]);
        let answers = answer(&form, &content).expect("accepted content maps");
        assert_eq!(answers.get("q0"), Some(&json!("custom")));
        // Multiselect: custom STARTS the selection when none was picked.
        let content = BTreeMap::from([(
            "q1_custom".to_string(),
            ElicitationContentValue::String("z".into()),
        )]);
        let answers0 = answer(&form, &content).expect("accepted content maps");
        assert_eq!(answers0.get("q1"), Some(&json!(["z"])));
    }

    #[test]
    fn absent_keys_are_skipped_and_number_booleans_pass_through() {
        let form = question(json!([
            { "key": "q0", "type": "number" },
            { "key": "q1", "type": "boolean" },
            { "key": "q2", "type": "string" },
        ]));
        let content = BTreeMap::from([
            ("q1".to_string(), ElicitationContentValue::Boolean(true)),
            ("q2".to_string(), ElicitationContentValue::Integer(4)), // wrong type for a string — surfaced as-is
        ]);
        let answer = answer(&form, &content).expect("accepted content maps");
        assert_eq!(answer.len(), 2);
        assert_eq!(answer.get("q1"), Some(&json!(true)));
        assert_eq!(answer.get("q2"), Some(&json!(4)));
        assert!(!answer.contains_key("q0"));
    }
}
