{
  description = "A Nix flake for simulating trade outcomes ";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }: 
    let
      system = "x86_64-linux"; # Change to "aarch64-linux" for ARM (e.g., Apple Silicon)
      pkgs = nixpkgs.legacyPackages.${system};
    in {
      packages.${system}.default = pkgs.python3Packages.buildPythonApplication {
        pname = "simulate-trades";
        version = "0.1.1";
        src = ./.;

        propagatedBuildInputs = [
          pkgs.python3Packages.numpy
          pkgs.python3Packages.mypy
          pkgs.python3Packages.scipy
          pkgs.python3Packages.matplotlib
          pkgs.python3Packages.yfinance
        ];


      };
    };
}
