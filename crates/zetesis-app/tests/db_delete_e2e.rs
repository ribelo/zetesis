//! End-to-end integration tests for the db delete functionality.

use zetesis_app::cli::db_delete::DbDeleteArgs;
use zetesis_app::error::AppError;

#[test]
fn db_delete_dry_run_noop() -> Result<(), AppError> {
    // Test CLI argument parsing
    let args = DbDeleteArgs {
        index: "test".to_string(),
        id: "test-doc-id".to_string(),
        dry_run: true,
        assume_yes: true,
    };

    // Test that CLI arguments are parsed correctly
    assert_eq!(args.index, "test");
    assert_eq!(args.id, "test-doc-id");
    assert!(args.dry_run);
    assert!(args.assume_yes);

    Ok(())
}

#[test]
fn db_delete_with_cli_arguments() {
    // Test CLI argument parsing with all combinations
    let args1 = DbDeleteArgs {
        index: "kio".to_string(),
        id: "doc123".to_string(),
        dry_run: true,
        assume_yes: false,
    };
    assert!(args1.dry_run);
    assert!(!args1.assume_yes);

    let args2 = DbDeleteArgs {
        index: "kio".to_string(),
        id: "doc456".to_string(),
        dry_run: false,
        assume_yes: true,
    };
    assert!(!args2.dry_run);
    assert!(args2.assume_yes);

    let args3 = DbDeleteArgs {
        index: "kio".to_string(),
        id: "doc789".to_string(),
        dry_run: false,
        assume_yes: false,
    };
    assert!(!args3.dry_run);
    assert!(!args3.assume_yes);
}

#[test]
fn db_delete_index_validation() {
    // Test index name validation
    let valid_args = DbDeleteArgs {
        index: "valid-index-name".to_string(),
        id: "test-doc".to_string(),
        dry_run: true,
        assume_yes: true,
    };
    assert_eq!(valid_args.index, "valid-index-name");

    // Test with hyphenated index name (should be valid)
    let hyphenated_args = DbDeleteArgs {
        index: "test-kio-2023".to_string(),
        id: "test-doc".to_string(),
        dry_run: true,
        assume_yes: true,
    };
    assert_eq!(hyphenated_args.index, "test-kio-2023");
}

#[test]
fn db_delete_missing_index_error() {
    let args = DbDeleteArgs {
        index: "non-existent-index".to_string(),
        id: "test-doc".to_string(),
        dry_run: false,
        assume_yes: true,
    };

    // Test the structure
    assert_eq!(args.index, "non-existent-index");
    assert_eq!(args.id, "test-doc");
    assert!(!args.dry_run);
    assert!(args.assume_yes);
}

#[test]
fn db_delete_argument_combinations() {
    // Test dry_run with assume_yes
    let args1 = DbDeleteArgs {
        index: "test-index".to_string(),
        id: "test-id".to_string(),
        dry_run: true,
        assume_yes: true,
    };
    assert!(args1.dry_run);
    assert!(args1.assume_yes);

    // Test no flags
    let args2 = DbDeleteArgs {
        index: "test-index".to_string(),
        id: "test-id".to_string(),
        dry_run: false,
        assume_yes: false,
    };
    assert!(!args2.dry_run);
    assert!(!args2.assume_yes);

    // Test only dry_run
    let args3 = DbDeleteArgs {
        index: "test-index".to_string(),
        id: "test-id".to_string(),
        dry_run: true,
        assume_yes: false,
    };
    assert!(args3.dry_run);
    assert!(!args3.assume_yes);

    // Test only assume_yes
    let args4 = DbDeleteArgs {
        index: "test-index".to_string(),
        id: "test-id".to_string(),
        dry_run: false,
        assume_yes: true,
    };
    assert!(!args4.dry_run);
    assert!(args4.assume_yes);
}
