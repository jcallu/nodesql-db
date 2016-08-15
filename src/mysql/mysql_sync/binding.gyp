{
  "targets": [
    {
      "target_name": "mysql_sync",
      "sources": [ "mysql_sync.cc"],
      'cflags_cc': [
        '-fexceptions',
      ],
      'conditions': [
        ['OS=="mac"', {
          'xcode_settings': {
            'GCC_ENABLE_CPP_EXCEPTIONS': 'YES'
          },
          "configurations": {
                "Debug": {
                    "xcode_settings": {
                        "OTHER_LDFLAGS": [
                            "-I/usr/local/opt/mysql/include",
                            "-L/usr/local/opt/mysql/lib"
                        ]
                    }
                },
                "Release": {
                    "xcode_settings": {
                        "OTHER_LDFLAGS": [
                            "-I/usr/local/include",
                            "-L/usr/local/lib"
                        ]
                    }
                }
            }
        }]
      ]
    }
  ]
}
