# JSON Schema Validation Examples

This directory contains comprehensive examples demonstrating all the validation features implemented in the JSON Schema form builder.

## Files

### Schema Examples

1. **`comprehensive-validation.json`** - Complete example showcasing all JSON Schema 2020-12 validation features:
   - Personal information with string validations (minLength, maxLength, pattern, format)
   - Address with conditional validations based on country
   - Preferences with array validations and enum constraints
   - Financial information with numeric validations and conditional requirements
   - Skills array with object validation
   - Portfolio with anyOf compositions
   - Terms acceptance with const validation

2. **`validation-features.json`** - Organized examples by validation category:
   - **String Validations**: Basic length limits, pattern matching, format validation (email, URL, date), enum selection
   - **Numeric Validations**: Integer ranges, number with exclusive bounds, multipleOf, range sliders
   - **Array Validations**: Multi-select with constraints, dynamic object arrays
   - **Conditional Validations**: if/then/else logic based on user type selection
   - **Dependent Fields**: dependentRequired, conditional field requirements
   - **Composition Validations**: anyOf for flexible contact methods, oneOf for exclusive payment methods
   - **Advanced Features**: const fields, boolean checkboxes, read-only fields, textarea with maxLength

3. **`complex.json`** - Original company registration form with nested objects and grouped fields

### Demo Server

**`main.go`** - HTTP server that renders the schema examples into interactive HTML forms with:
- Real-time client-side validation
- HTML5 form controls with validation attributes
- Tailwind CSS styling
- JavaScript validation feedback
- Form submission handling

## Validation Features Demonstrated

### String Validations
- `minLength` / `maxLength` - Length constraints
- `pattern` - Regular expression validation
- `format` - Built-in formats (email, uri, date, etc.)
- `enum` - Predefined value lists

### Numeric Validations
- `minimum` / `maximum` - Inclusive bounds
- `exclusiveMinimum` / `exclusiveMaximum` - Exclusive bounds
- `multipleOf` - Value must be multiple of specified number
- Integer vs Number types

### Array Validations
- `minItems` / `maxItems` - Size constraints
- `uniqueItems` - No duplicate values
- `items` - Schema for array elements
- Multi-select form controls

### Object Validations
- `required` - Required properties
- `dependentRequired` - Fields required based on other fields
- `dependentSchemas` - Schema changes based on other fields
- Nested object structures

### Conditional Logic
- `if` / `then` / `else` - Conditional schema application
- `allOf` - Must satisfy all sub-schemas
- `anyOf` - Must satisfy at least one sub-schema
- `oneOf` - Must satisfy exactly one sub-schema

### Advanced Features
- `const` - Constant values
- `default` - Default values
- `readOnly` - Read-only fields
- Boolean validation
- HTML form grouping and styling

## Running the Demo

1. Navigate to the validation-demo directory:
   ```bash
   cd examples/validation-demo
   ```

2. Run the server:
   ```bash
   go run main.go
   ```

3. Open your browser to `http://localhost:8080`

4. Explore the different validation examples:
   - Comprehensive Validation Demo - Complete real-world form
   - Validation Features Demo - Organized by validation type
   - Complex Form Demo - Original nested object example

## Implementation Details

The validation system is implemented in:
- `/renderer/validation.go` - Core validation logic and HTML attribute generation
- `/renderer/schema.go` - Form rendering with validation integration
- Client-side JavaScript - Real-time validation feedback

All examples demonstrate both server-side schema validation and client-side HTML5 validation attributes working together.
