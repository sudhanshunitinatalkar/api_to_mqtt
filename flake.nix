{
  description = "Datalogger";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-25.11";
  };

  outputs = { self, nixpkgs }:
    let
      # 1. Define the architectures you want to support
      supportedSystems = [ "x86_64-linux" "aarch64-linux" ];

      # 2. Create a helper function to generate attributes for every system
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
    in
    {
      # 3. Use the helper to generate devShells for both architectures
      devShells = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              python312
              python312Packages.paho-mqtt
              python312Packages.requests
              mosquitto
            ];
          };
        }
      );
    };
}