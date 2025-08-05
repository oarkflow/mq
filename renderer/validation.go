package renderer

import (
	"fmt"
	"strings"

	"github.com/oarkflow/jsonschema"
)

// ValidationInfo holds comprehensive validation information for a field
type ValidationInfo struct {
	// Basic validations
	Required      bool
	MinLength     *float64
	MaxLength     *float64
	Minimum       *jsonschema.Rat
	Maximum       *jsonschema.Rat
	Pattern       string
	Format        string
	Enum          []interface{}
	MultipleOf    *jsonschema.Rat
	ExclusiveMin  *jsonschema.Rat
	ExclusiveMax  *jsonschema.Rat
	MinItems      *float64
	MaxItems      *float64
	UniqueItems   bool
	MinProperties *float64
	MaxProperties *float64
	Const         interface{}

	// Advanced JSON Schema 2020-12 validations
	AllOf                 []*jsonschema.Schema
	AnyOf                 []*jsonschema.Schema
	OneOf                 []*jsonschema.Schema
	Not                   *jsonschema.Schema
	If                    *jsonschema.Schema
	Then                  *jsonschema.Schema
	Else                  *jsonschema.Schema
	DependentSchemas      map[string]*jsonschema.Schema
	DependentRequired     map[string][]string
	PrefixItems           []*jsonschema.Schema
	Items                 *jsonschema.Schema
	Contains              *jsonschema.Schema
	MaxContains           *float64
	MinContains           *float64
	UnevaluatedItems      *jsonschema.Schema
	UnevaluatedProperties *jsonschema.Schema
	PropertyNames         *jsonschema.Schema
	AdditionalProperties  *jsonschema.Schema
	PatternProperties     *jsonschema.SchemaMap

	// Content validations
	ContentEncoding  *string
	ContentMediaType *string
	ContentSchema    *jsonschema.Schema

	// Metadata
	Title       *string
	Description *string
	Default     interface{}
	Examples    []interface{}
	Deprecated  *bool
	ReadOnly    *bool
	WriteOnly   *bool
}

// extractValidationInfo extracts comprehensive validation information from JSON Schema
func extractValidationInfo(schema *jsonschema.Schema, isRequired bool) ValidationInfo {
	validation := ValidationInfo{
		Required: isRequired,
	}

	// Basic string validations
	if schema.MinLength != nil {
		validation.MinLength = schema.MinLength
	}
	if schema.MaxLength != nil {
		validation.MaxLength = schema.MaxLength
	}
	if schema.Pattern != nil {
		validation.Pattern = *schema.Pattern
	}
	if schema.Format != nil {
		validation.Format = *schema.Format
	}

	// Numeric validations
	if schema.Minimum != nil {
		validation.Minimum = schema.Minimum
	}
	if schema.Maximum != nil {
		validation.Maximum = schema.Maximum
	}
	if schema.ExclusiveMinimum != nil {
		validation.ExclusiveMin = schema.ExclusiveMinimum
	}
	if schema.ExclusiveMaximum != nil {
		validation.ExclusiveMax = schema.ExclusiveMaximum
	}
	if schema.MultipleOf != nil {
		validation.MultipleOf = schema.MultipleOf
	}

	// Array validations
	if schema.MinItems != nil {
		validation.MinItems = schema.MinItems
	}
	if schema.MaxItems != nil {
		validation.MaxItems = schema.MaxItems
	}
	if schema.UniqueItems != nil {
		validation.UniqueItems = *schema.UniqueItems
	}
	if schema.MaxContains != nil {
		validation.MaxContains = schema.MaxContains
	}
	if schema.MinContains != nil {
		validation.MinContains = schema.MinContains
	}

	// Object validations
	if schema.MinProperties != nil {
		validation.MinProperties = schema.MinProperties
	}
	if schema.MaxProperties != nil {
		validation.MaxProperties = schema.MaxProperties
	}

	// Enum and const values
	if schema.Enum != nil {
		validation.Enum = schema.Enum
	}
	if schema.Const != nil {
		validation.Const = *schema.Const
	}

	// Advanced JSON Schema 2020-12 features
	if schema.AllOf != nil {
		validation.AllOf = schema.AllOf
	}
	if schema.AnyOf != nil {
		validation.AnyOf = schema.AnyOf
	}
	if schema.OneOf != nil {
		validation.OneOf = schema.OneOf
	}
	if schema.Not != nil {
		validation.Not = schema.Not
	}

	// Conditional validations
	if schema.If != nil {
		validation.If = schema.If
	}
	if schema.Then != nil {
		validation.Then = schema.Then
	}
	if schema.Else != nil {
		validation.Else = schema.Else
	}

	// Dependent validations
	if schema.DependentSchemas != nil {
		validation.DependentSchemas = schema.DependentSchemas
	}
	if schema.DependentRequired != nil {
		validation.DependentRequired = schema.DependentRequired
	}

	// Array item validations
	if schema.PrefixItems != nil {
		validation.PrefixItems = schema.PrefixItems
	}
	if schema.Items != nil {
		validation.Items = schema.Items
	}
	if schema.Contains != nil {
		validation.Contains = schema.Contains
	}
	if schema.UnevaluatedItems != nil {
		validation.UnevaluatedItems = schema.UnevaluatedItems
	}

	// Property validations
	if schema.PropertyNames != nil {
		validation.PropertyNames = schema.PropertyNames
	}
	if schema.AdditionalProperties != nil {
		validation.AdditionalProperties = schema.AdditionalProperties
	}
	if schema.PatternProperties != nil {
		validation.PatternProperties = schema.PatternProperties
	}
	if schema.UnevaluatedProperties != nil {
		validation.UnevaluatedProperties = schema.UnevaluatedProperties
	}

	// Content validations
	if schema.ContentEncoding != nil {
		validation.ContentEncoding = schema.ContentEncoding
	}
	if schema.ContentMediaType != nil {
		validation.ContentMediaType = schema.ContentMediaType
	}
	if schema.ContentSchema != nil {
		validation.ContentSchema = schema.ContentSchema
	}

	// Metadata
	if schema.Title != nil {
		validation.Title = schema.Title
	}
	if schema.Description != nil {
		validation.Description = schema.Description
	}
	if schema.Default != nil {
		validation.Default = schema.Default
	}
	if schema.Examples != nil {
		validation.Examples = schema.Examples
	}
	if schema.Deprecated != nil {
		validation.Deprecated = schema.Deprecated
	}
	if schema.ReadOnly != nil {
		validation.ReadOnly = schema.ReadOnly
	}
	if schema.WriteOnly != nil {
		validation.WriteOnly = schema.WriteOnly
	}

	return validation
}

// generateValidationAttributes creates comprehensive HTML validation attributes
func generateValidationAttributes(validation ValidationInfo) []string {
	var attrs []string

	if validation.Required {
		attrs = append(attrs, `required="required"`)
	}

	// String validations
	if validation.MinLength != nil {
		attrs = append(attrs, fmt.Sprintf(`minlength="%.0f"`, *validation.MinLength))
	}
	if validation.MaxLength != nil {
		attrs = append(attrs, fmt.Sprintf(`maxlength="%.0f"`, *validation.MaxLength))
	}
	if validation.Pattern != "" {
		attrs = append(attrs, fmt.Sprintf(`pattern="%s"`, validation.Pattern))
	}

	// Numeric validations
	if validation.Minimum != nil {
		minVal, _ := validation.Minimum.Float64()
		attrs = append(attrs, fmt.Sprintf(`min="%g"`, minVal))
	}
	if validation.Maximum != nil {
		maxVal, _ := validation.Maximum.Float64()
		attrs = append(attrs, fmt.Sprintf(`max="%g"`, maxVal))
	}
	if validation.ExclusiveMin != nil {
		exclusiveMinVal, _ := validation.ExclusiveMin.Float64()
		attrs = append(attrs, fmt.Sprintf(`data-exclusive-min="%g"`, exclusiveMinVal))
	}
	if validation.ExclusiveMax != nil {
		exclusiveMaxVal, _ := validation.ExclusiveMax.Float64()
		attrs = append(attrs, fmt.Sprintf(`data-exclusive-max="%g"`, exclusiveMaxVal))
	}
	if validation.MultipleOf != nil {
		stepVal, _ := validation.MultipleOf.Float64()
		attrs = append(attrs, fmt.Sprintf(`step="%g"`, stepVal))
	}

	// Array validations (for multi-select and array inputs)
	if validation.MinItems != nil {
		attrs = append(attrs, fmt.Sprintf(`data-min-items="%.0f"`, *validation.MinItems))
	}
	if validation.MaxItems != nil {
		attrs = append(attrs, fmt.Sprintf(`data-max-items="%.0f"`, *validation.MaxItems))
	}
	if validation.UniqueItems {
		attrs = append(attrs, `data-unique-items="true"`)
	}

	// Metadata attributes
	if validation.Title != nil {
		attrs = append(attrs, fmt.Sprintf(`title="%s"`, *validation.Title))
	}
	if validation.Description != nil {
		attrs = append(attrs, fmt.Sprintf(`data-description="%s"`, *validation.Description))
	}
	if validation.Default != nil {
		attrs = append(attrs, fmt.Sprintf(`data-default="%v"`, validation.Default))
	}
	if validation.ReadOnly != nil && *validation.ReadOnly {
		attrs = append(attrs, `readonly="readonly"`)
	}
	if validation.Deprecated != nil && *validation.Deprecated {
		attrs = append(attrs, `data-deprecated="true"`)
	}

	// Complex validation indicators (for client-side handling)
	if len(validation.AllOf) > 0 {
		attrs = append(attrs, `data-has-allof="true"`)
	}
	if len(validation.AnyOf) > 0 {
		attrs = append(attrs, `data-has-anyof="true"`)
	}
	if len(validation.OneOf) > 0 {
		attrs = append(attrs, `data-has-oneof="true"`)
	}
	if validation.Not != nil {
		attrs = append(attrs, `data-has-not="true"`)
	}
	if validation.If != nil {
		attrs = append(attrs, `data-has-conditional="true"`)
	}
	if len(validation.DependentRequired) > 0 {
		attrs = append(attrs, `data-has-dependent-required="true"`)
	}

	return attrs
}

// generateClientSideValidation creates comprehensive JavaScript validation functions
func generateClientSideValidation(fieldPath string, validation ValidationInfo, allFields map[string]ValidationInfo) string {
	if !hasValidation(validation) {
		return ""
	}

	var validations []string
	var dependentValidations []string

	// Basic validations
	if validation.Required {
		validations = append(validations, `
			if (!value || (typeof value === 'string' && value.trim() === '')) {
				return 'This field is required';
			}`)
	}

	// String validations
	if validation.MinLength != nil {
		validations = append(validations, fmt.Sprintf(`
			if (value && value.length < %.0f) {
				return 'Minimum length is %.0f characters';
			}`, *validation.MinLength, *validation.MinLength))
	}
	if validation.MaxLength != nil {
		validations = append(validations, fmt.Sprintf(`
			if (value && value.length > %.0f) {
				return 'Maximum length is %.0f characters';
			}`, *validation.MaxLength, *validation.MaxLength))
	}
	if validation.Pattern != "" {
		validations = append(validations, fmt.Sprintf(`
			if (value && !/%s/.test(value)) {
				return 'Invalid format';
			}`, strings.ReplaceAll(validation.Pattern, `\`, `\\`)))
	}

	// Numeric validations
	if validation.Minimum != nil {
		minVal, _ := validation.Minimum.Float64()
		validations = append(validations, fmt.Sprintf(`
			if (value !== '' && parseFloat(value) < %g) {
				return 'Minimum value is %g';
			}`, minVal, minVal))
	}
	if validation.Maximum != nil {
		maxVal, _ := validation.Maximum.Float64()
		validations = append(validations, fmt.Sprintf(`
			if (value !== '' && parseFloat(value) > %g) {
				return 'Maximum value is %g';
			}`, maxVal, maxVal))
	}
	if validation.ExclusiveMin != nil {
		exclusiveMinVal, _ := validation.ExclusiveMin.Float64()
		validations = append(validations, fmt.Sprintf(`
			if (value !== '' && parseFloat(value) <= %g) {
				return 'Value must be greater than %g';
			}`, exclusiveMinVal, exclusiveMinVal))
	}
	if validation.ExclusiveMax != nil {
		exclusiveMaxVal, _ := validation.ExclusiveMax.Float64()
		validations = append(validations, fmt.Sprintf(`
			if (value !== '' && parseFloat(value) >= %g) {
				return 'Value must be less than %g';
			}`, exclusiveMaxVal, exclusiveMaxVal))
	}

	// Enum validations
	if len(validation.Enum) > 0 {
		enumValues := make([]string, len(validation.Enum))
		for i, v := range validation.Enum {
			enumValues[i] = fmt.Sprintf("'%v'", v)
		}
		validations = append(validations, fmt.Sprintf(`
			var allowedValues = [%s];
			if (value && allowedValues.indexOf(value) === -1) {
				return "Value must be one of: %s";
			}`, strings.Join(enumValues, ", "), strings.Join(enumValues, ", ")))
	}

	// Const validation
	if validation.Const != nil {
		validations = append(validations, fmt.Sprintf(`
			if (value !== '%v') {
				return 'Value must be %v';
			}`, validation.Const, validation.Const))
	}

	// Array validations
	if validation.MinItems != nil {
		validations = append(validations, fmt.Sprintf(`
			if (Array.isArray(value) && value.length < %.0f) {
				return 'Minimum %.0f items required';
			}`, *validation.MinItems, *validation.MinItems))
	}
	if validation.MaxItems != nil {
		validations = append(validations, fmt.Sprintf(`
			if (Array.isArray(value) && value.length > %.0f) {
				return 'Maximum %.0f items allowed';
			}`, *validation.MaxItems, *validation.MaxItems))
	}
	if validation.UniqueItems {
		validations = append(validations, `
			if (Array.isArray(value)) {
				var unique = [...new Set(value)];
				if (unique.length !== value.length) {
					return 'All items must be unique';
				}
			}`)
	}

	// Dependent required validations
	if len(validation.DependentRequired) > 0 {
		for depField, requiredFields := range validation.DependentRequired {
			for _, reqField := range requiredFields {
				dependentValidations = append(dependentValidations, fmt.Sprintf(`
					// Check if %s has value, then %s is required
					var depField = document.querySelector('[name="%s"]');
					var reqField = document.querySelector('[name="%s"]');
					if (depField && depField.value && (!reqField || !reqField.value)) {
						return 'Field %s is required when %s has a value';
					}`, depField, reqField, reqField, depField))
			}
		}
	}

	// Complex validations warning (these need server-side validation)
	var complexValidationWarnings []string
	if len(validation.AllOf) > 0 {
		complexValidationWarnings = append(complexValidationWarnings, "// AllOf validation requires server-side validation")
	}
	if len(validation.AnyOf) > 0 {
		complexValidationWarnings = append(complexValidationWarnings, "// AnyOf validation requires server-side validation")
	}
	if len(validation.OneOf) > 0 {
		complexValidationWarnings = append(complexValidationWarnings, "// OneOf validation requires server-side validation")
	}
	if validation.Not != nil {
		complexValidationWarnings = append(complexValidationWarnings, "// Not validation requires server-side validation")
	}
	if validation.If != nil {
		complexValidationWarnings = append(complexValidationWarnings, "// Conditional validation requires server-side validation")
	}

	if len(validations) == 0 && len(dependentValidations) == 0 {
		return ""
	}

	fieldId := strings.ReplaceAll(fieldPath, ".", "_")
	allValidations := append(validations, dependentValidations...)
	if len(complexValidationWarnings) > 0 {
		allValidations = append([]string{strings.Join(complexValidationWarnings, "\n\t\t\t")}, allValidations...)
	}

	return fmt.Sprintf(`
		<script>
		(function() {
			var field = document.querySelector('[name="%s"]');
			if (!field) return;

			function validate%s(value) {
				%s
				return null;
			}

			function validateForm() {
				// Dependent validations that check other fields
				%s
				return null;
			}

			field.addEventListener('blur', function() {
				var error = validate%s(this.value) || validateForm();
				var errorElement = document.getElementById('%s_error');

				if (error) {
					if (!errorElement) {
						errorElement = document.createElement('div');
						errorElement.id = '%s_error';
						errorElement.className = 'validation-error';
						errorElement.style.color = 'red';
						errorElement.style.fontSize = '0.875rem';
						errorElement.style.marginTop = '0.25rem';
						this.parentNode.appendChild(errorElement);
					}
					errorElement.textContent = error;
					this.classList.add('invalid');
					this.style.borderColor = 'red';
				} else {
					if (errorElement) {
						errorElement.remove();
					}
					this.classList.remove('invalid');
					this.style.borderColor = '';
				}
			});

			// Also validate on input for immediate feedback
			field.addEventListener('input', function() {
				if (this.classList.contains('invalid')) {
					var error = validate%s(this.value);
					var errorElement = document.getElementById('%s_error');
					if (!error && errorElement) {
						errorElement.remove();
						this.classList.remove('invalid');
						this.style.borderColor = '';
					}
				}
			});
		})();
		</script>
	`, fieldPath, fieldId, strings.Join(allValidations, "\n"),
		strings.Join(dependentValidations, "\n"), fieldId, fieldId, fieldId, fieldId, fieldId)
}

// hasValidation checks if validation info contains any validation rules
func hasValidation(validation ValidationInfo) bool {
	return validation.Required ||
		validation.MinLength != nil ||
		validation.MaxLength != nil ||
		validation.Minimum != nil ||
		validation.Maximum != nil ||
		validation.Pattern != "" ||
		validation.ExclusiveMin != nil ||
		validation.ExclusiveMax != nil ||
		validation.MultipleOf != nil ||
		len(validation.Enum) > 0 ||
		validation.Const != nil ||
		validation.MinItems != nil ||
		validation.MaxItems != nil ||
		validation.UniqueItems ||
		validation.MinProperties != nil ||
		validation.MaxProperties != nil ||
		len(validation.AllOf) > 0 ||
		len(validation.AnyOf) > 0 ||
		len(validation.OneOf) > 0 ||
		validation.Not != nil ||
		validation.If != nil ||
		validation.Then != nil ||
		validation.Else != nil ||
		len(validation.DependentSchemas) > 0 ||
		len(validation.DependentRequired) > 0 ||
		len(validation.PrefixItems) > 0 ||
		validation.Items != nil ||
		validation.Contains != nil ||
		validation.MaxContains != nil ||
		validation.MinContains != nil ||
		validation.UnevaluatedItems != nil ||
		validation.UnevaluatedProperties != nil ||
		validation.PropertyNames != nil ||
		validation.AdditionalProperties != nil ||
		validation.PatternProperties != nil ||
		validation.ContentEncoding != nil ||
		validation.ContentMediaType != nil ||
		validation.ContentSchema != nil ||
		validation.ReadOnly != nil ||
		validation.WriteOnly != nil ||
		validation.Deprecated != nil
}

// getInputTypeFromSchema determines the appropriate HTML input type based on JSON Schema
func getInputTypeFromSchema(schema *jsonschema.Schema) string {
	// Check UI type first
	if schema.UI != nil {
		if uiType, ok := schema.UI["type"].(string); ok {
			return uiType
		}
	}

	// Check format
	if schema.Format != nil {
		switch *schema.Format {
		case "email":
			return "email"
		case "uri", "uri-reference":
			return "url"
		case "date":
			return "date"
		case "time":
			return "time"
		case "date-time":
			return "datetime-local"
		case "password":
			return "password"
		}
	}

	// Check type
	var typeStr string
	if len(schema.Type) > 0 {
		typeStr = schema.Type[0]
	}

	switch typeStr {
	case "string":
		return "text"
	case "number":
		return "number"
	case "integer":
		return "number"
	case "boolean":
		return "checkbox"
	default:
		return "text"
	}
}

// shouldUseTextarea determines if a string field should use textarea
func shouldUseTextarea(schema *jsonschema.Schema) bool {
	if schema.UI != nil {
		if element, ok := schema.UI["element"].(string); ok {
			return element == "textarea"
		}
		if rows, ok := schema.UI["rows"]; ok && rows != nil {
			return true
		}
	}

	// Use textarea for long strings
	if schema.MaxLength != nil && *schema.MaxLength > 255 {
		return true
	}

	return false
}

// shouldUseSelect determines if a field should use select element
func shouldUseSelect(schema *jsonschema.Schema) bool {
	if schema.UI != nil {
		if element, ok := schema.UI["element"].(string); ok {
			return element == "select"
		}
	}

	// Use select for enum values
	return len(schema.Enum) > 0
}
