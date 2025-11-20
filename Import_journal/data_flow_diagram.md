# Data Flow Diagram

## Current Implementation Flow

```mermaid
flowchart TD
    A[Excel File<br/>11 columns] --> B[read_excel_file]
    B --> C[Process each row]
    C --> D[Create 1 journal line per row]
    D --> E[Create account.move]
    
    subgraph "Current Columns"
        F[document_number]
        G[date]
        H[journal]
        I[reference]
        J[account1]
        K[account2]
        L[partner_code]
        M[partner_name]
        N[label]
        O[debit]
        P[credit]
    end
```

## New Implementation Flow

```mermaid
flowchart TD
    A[Excel File<br/>13 columns] --> B[read_excel_file]
    B --> C[Process each row]
    C --> D[Create 2 journal lines per row]
    D --> E[Debit Line<br/>account_debit]
    D --> F[Credit Line<br/>account_credit]
    E --> G[Create account.move]
    F --> G
    
    subgraph "New Columns"
        H[document_number]
        I[date]
        J[journal]
        K[reference]
        L[custom_reference]
        M[account_debit]
        N[account_credit]
        O[partner_code]
        P[old_partner_code]
        Q[partner_name]
        R[label]
        S[debit]
        T[credit]
    end
```

## Key Changes

1. **Column Count**: 11 → 13 columns
2. **Account Handling**: Single account → Separate debit/credit accounts
3. **Line Creation**: 1 line/row → 2 lines/row
4. **New Fields**: custom_reference, old_partner_code
5. **Partner Lookup**: Enhanced with fallback mechanism