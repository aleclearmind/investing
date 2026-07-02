{
  description = "A Nix flake for simulating trade outcomes ";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux"; # Change to "aarch64-linux" for ARM (e.g., Apple Silicon)
      pkgs = nixpkgs.legacyPackages.${system};
      pythonEnv = pkgs.python3.withPackages (ps: [
        ps.numpy
        ps.scipy
        ps.mypy
        ps.requests
        ps.curl-cffi
        ps.playwright
      ]);
    in {
      # Environment used to run ./configure (fetch data + generate the build).
      devShells.${system}.default = pkgs.mkShell {
        packages = [ pythonEnv pkgs.ninja pkgs.curl ];

        # Use the browser bundled with nixpkgs instead of downloading one.
        PLAYWRIGHT_BROWSERS_PATH = pkgs.playwright-driver.browsers;
        PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS = "true";
      };

      packages.${system}.default = pkgs.python3Packages.buildPythonApplication {
        pname = "simulate-trades";
        version = "0.1.1";
        src = ./.;

        propagatedBuildInputs = [
          pkgs.python3Packages.numpy
          pkgs.python3Packages.mypy
          pkgs.python3Packages.requests
          pkgs.python3Packages.curl-cffi
          pkgs.python3Packages.playwright
          pkgs.python3Packages.scipy
        ];
      };
    };
}
