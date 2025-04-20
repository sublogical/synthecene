use assert_fs::prelude::*;

#[test]
fn test_create_universe() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("vera-config")?;
    cmd.arg("create-universe").arg("test/config/basic_universe.txtpb");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("[SUCCESS]"));

    Ok(())

    let mut cmd = Command::cargo_bin("vera-config")?;
    cmd.arg("delete-universe").arg("test/config/basic_universe.txtpb");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("[SUCCESS]"));

    Ok(())


}

#[test]
fn test_create_table() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("vera-config")?;
    cmd.arg("create-universe").arg("test/config/basic_universe.txtpb");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("[SUCCESS]"));
    
    let mut cmd = Command::cargo_bin("vera-config")?;
    cmd.arg("create-table").arg("test/config/basic_table.txtpb");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("[SUCCESS]"));

    Ok(())
}

#[test]
fn test_create_type() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("vera-config")?;
    cmd.arg("create-universe").arg("test/config/basic_universe.txtpb");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("[SUCCESS]"));
    
    let mut cmd = Command::cargo_bin("vera-config")?;
    cmd.arg("create-type").arg("test/config/basic_types.txtpb");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("[SUCCESS]"));

    Ok(())
}

#[test]